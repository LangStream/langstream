package com.datastax.oss.sga.deployer.k8s.controllers;

import com.datastax.oss.sga.deployer.k8s.DeployerConfiguration;
import com.datastax.oss.sga.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import com.datastax.oss.sga.deployer.k8s.api.crds.apps.ApplicationSpec;
import com.datastax.oss.sga.deployer.k8s.util.KubeUtil;
import com.datastax.oss.sga.deployer.k8s.util.SerializationUtil;
import com.datastax.oss.sga.deployer.k8s.util.SpecDiffer;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EmptyDirVolumeSource;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Cleaner;
import io.javaoperatorsdk.operator.api.reconciler.Constants;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
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
public class AppController implements Reconciler<ApplicationCustomResource>, Cleaner<ApplicationCustomResource> {

    @Inject
    KubernetesClient client;

    @Inject
    DeployerConfiguration configuration;

    @Override
    public UpdateControl<ApplicationCustomResource> reconcile(ApplicationCustomResource application,
                                                              Context<ApplicationCustomResource> context)
            throws Exception {
        final boolean reschedule = handleJob(application, false);
        return reschedule ? UpdateControl.updateStatus(application)
                .rescheduleAfter(5, TimeUnit.SECONDS) : UpdateControl.updateStatus(application);
    }

    @Override
    public DeleteControl cleanup(ApplicationCustomResource application,
                                 Context<ApplicationCustomResource> context) {

        final boolean reschedule = handleJob(application, true);
        return reschedule ? DeleteControl.defaultDelete()
                .rescheduleAfter(5, TimeUnit.SECONDS) : DeleteControl.defaultDelete();
    }

    private boolean handleJob(ApplicationCustomResource application, boolean delete) {
        final String tenant = application.getSpec().getTenant();
        final String appName = application.getMetadata().getName();

        final ApplicationSpec spec = application.getSpec();
        final String jobName;
        if (delete) {
            jobName = "sga-runtime-deployer-cleanup-" + appName;
        } else {
            jobName = "sga-runtime-deployer-" + appName;
        }
        final String targetNamespace = configuration.namespacePrefix() + tenant;
        final Job currentJob = client.batch().v1().jobs()
                .inNamespace(targetNamespace)
                .withName(jobName)
                .get();


        if (currentJob == null) {
            createJob(tenant, appName, spec, jobName, targetNamespace, delete);
            return true;
        } else {
            if (KubeUtil.isJobCompleted(currentJob)) {
                application.getStatus().setLastApplied(SerializationUtil.writeAsJson(spec));
                return false;
            } else {
                return true;
            }
        }
    }

    @SneakyThrows
    private void createJob(String tenant, String name, ApplicationSpec spec, String jobName, String targetNamespace,
                           boolean delete) {

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
        final String command = delete ? "delete" : "deploy";

        final Container container = new ContainerBuilder()
                .withName("deployer")
                .withImage(spec.getImage())
                .withImagePullPolicy(spec.getImagePullPolicy())
                .withArgs("deployer-runtime", command, "/app-config/config", "/app-secrets/secrets")
                .withVolumeMounts(new VolumeMountBuilder()
                                .withName("app-config")
                                .withMountPath("/app-config")
                                .build(),
                        new VolumeMountBuilder()
                                .withName("app-secrets")
                                .withMountPath("/app-secrets")
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
                                .build(),
                        new VolumeBuilder()
                                .withName("app-secrets")
                                .withNewSecret()
                                .withSecretName(name)
                                .endSecret()
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
