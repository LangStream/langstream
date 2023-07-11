package com.datastax.oss.sga.deployer.k8s.controllers;

import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.deployer.k8s.DeployerConfiguration;
import com.datastax.oss.sga.deployer.k8s.api.crds.apps.Application;
import com.datastax.oss.sga.deployer.k8s.api.crds.apps.ApplicationSpec;
import com.datastax.oss.sga.deployer.k8s.util.KubeUtil;
import com.datastax.oss.sga.deployer.k8s.util.SerializationUtil;
import com.datastax.oss.sga.deployer.k8s.util.SpecDiffer;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Constants;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.extern.jbosslog.JBossLog;

@ControllerConfiguration(namespaces = Constants.WATCH_ALL_NAMESPACES, name = "app-controller")
@JBossLog
public class AppController implements Reconciler<Application> {

    @Inject
    KubernetesClient client;

    @Inject
    DeployerConfiguration configuration;

    @Override
    public UpdateControl<Application> reconcile(Application application, Context<Application> context)
            throws Exception {
        final String tenant = application.getSpec().getTenant();
        final String appName = application.getMetadata().getName();

        final ApplicationSpec spec = application.getSpec();
        final String jobName = "init" + appName;
        final String targetNamespace = "" + configuration.namespacePrefix() + tenant;
        final Job currentJob = client.batch().v1().jobs()
                .inNamespace(targetNamespace)
                .withName(jobName)
                .get();


        if (currentJob == null || areSpecChanged(application)) {
            if (currentJob != null) {
                client.resource(currentJob).inNamespace(targetNamespace).delete();
            }
            createJob(tenant, spec, jobName, targetNamespace);
            return UpdateControl.updateStatus(application);
        } else {
            if (KubeUtil.isJobCompleted(currentJob)) {
                application.getStatus().setLastApplied(SerializationUtil.writeAsJson(spec));
                return UpdateControl.updateStatus(application);
            } else {
                return UpdateControl.updateStatus(application)
                        .rescheduleAfter(5, TimeUnit.SECONDS);
            }
        }


    }

    private void createJob(String tenant, ApplicationSpec spec, String jobName, String targetNamespace) {
        final ApplicationInstance instance = new ApplicationInstance();
        instance.setInstance(spec.getInstance());
        instance.setModules(spec.getModules());
        instance.setResources(spec.getResources());


        final Container container = new ContainerBuilder()
                .withName("initialize")
                .withImage(spec.getImage())
                .withImagePullPolicy(spec.getImagePullPolicy())
                .withNewResources()
                .withRequests(Map.of("cpu", Quantity.parse("100m"), "memory", Quantity.parse("128Mi")))
                .endResources()
                .build();


        final Job job = new JobBuilder()
                .withNewMetadata()
                .withName(jobName)
                .withNamespace(targetNamespace)
                .withLabels(Map.of("app", "sga", "tenant", tenant))
                .endMetadata()
                .withNewSpec()
                .withNewTemplate()
                .withNewMetadata()
                .withLabels(Map.of("app", "sga", "tenant", tenant))
                .endMetadata()
                .withNewSpec()
                .withContainers(List.of(container))
                .withRestartPolicy("OnFailure")
                .endSpec()
                .endTemplate()
                .endSpec()
                .build();

        KubeUtil.patchJob(client, job);
    }

    protected boolean areSpecChanged(Application cr) {
        final String lastApplied = cr.getStatus().getLastApplied();
        if (lastApplied == null) {
            return true;
        }
        return !SpecDiffer.generateDiff(cr.getSpec(), lastApplied).areEquals();
    }

}
