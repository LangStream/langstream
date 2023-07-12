package com.datastax.oss.sga.deployer.k8s.controllers;

import com.datastax.oss.sga.deployer.k8s.DeployerConfiguration;
import com.datastax.oss.sga.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import com.datastax.oss.sga.deployer.k8s.api.crds.apps.ApplicationSpec;
import com.datastax.oss.sga.deployer.k8s.util.KubeUtil;
import com.datastax.oss.sga.deployer.k8s.util.SerializationUtil;
import com.datastax.oss.sga.deployer.k8s.util.SpecDiffer;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EmptyDirVolumeSource;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
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
import lombok.SneakyThrows;
import lombok.extern.jbosslog.JBossLog;

@ControllerConfiguration(namespaces = Constants.WATCH_ALL_NAMESPACES, name = "app-controller")
@JBossLog
public class AppController implements Reconciler<ApplicationCustomResource> {

    @Inject
    KubernetesClient client;

    @Inject
    DeployerConfiguration configuration;

    @Override
    public UpdateControl<ApplicationCustomResource> reconcile(ApplicationCustomResource application, Context<ApplicationCustomResource> context)
            throws Exception {
        final String tenant = application.getSpec().getTenant();
        final String appName = application.getMetadata().getName();

        final ApplicationSpec spec = application.getSpec();
        final String jobName = "sga-runtime-deployer-" + appName;
        final String targetNamespace = configuration.namespacePrefix() + tenant;
        final Job currentJob = client.batch().v1().jobs()
                .inNamespace(targetNamespace)
                .withName(jobName)
                .get();


        if (currentJob == null || areSpecChanged(application)) {
            if (currentJob != null) {
                client.resource(currentJob).inNamespace(targetNamespace).delete();
            }
            createJob(tenant, appName, spec, jobName, targetNamespace);
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

    @SneakyThrows
    private void createJob(String tenant, String name, ApplicationSpec spec, String jobName, String targetNamespace) {

        final Map<String, String> config = Map.of("name", name, "application", spec.getApplication());

        final Container initContainer = new ContainerBuilder()
                .withName("deployer-init-config")
                .withImage(spec.getImage())
                .withImagePullPolicy(spec.getImagePullPolicy())
                .withCommand("bash", "-c")
                .withArgs("echo '%s' > /app-config/config".formatted(SerializationUtil.writeAsJson(config)))
                .withVolumeMounts(new VolumeMountBuilder()
                        .withName("app-config")
                        .withMountPath("/app-config")
                        .build())
                .build();
        final Container container = new ContainerBuilder()
                .withName("deployer")
                .withImage(spec.getImage())
                .withImagePullPolicy(spec.getImagePullPolicy())
                .withArgs("deployer-runtime", "/app-config/config")
                .withVolumeMounts(new VolumeMountBuilder()
                        .withName("app-config")
                        .withMountPath("/app-config")
                        .build())
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
                .withVolumes(new VolumeBuilder()
                        .withName("app-config")
                        .withEmptyDir(new EmptyDirVolumeSource())
                        .build()
                )
                .withInitContainers(List.of(initContainer))
                .withContainers(List.of(container))
                .withRestartPolicy("OnFailure")
                .endSpec()
                .endTemplate()
                .endSpec()
                .build();

        KubeUtil.patchJob(client, job);
    }

    protected boolean areSpecChanged(ApplicationCustomResource cr) {
        final String lastApplied = cr.getStatus().getLastApplied();
        if (lastApplied == null) {
            return true;
        }
        return !SpecDiffer.generateDiff(cr.getSpec(), lastApplied).areEquals();
    }

}
