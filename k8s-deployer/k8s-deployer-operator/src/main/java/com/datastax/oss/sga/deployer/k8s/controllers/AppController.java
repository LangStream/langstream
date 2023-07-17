package com.datastax.oss.sga.deployer.k8s.controllers;

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
import io.javaoperatorsdk.operator.api.reconciler.Constants;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.jbosslog.JBossLog;

@ControllerConfiguration(namespaces = Constants.WATCH_ALL_NAMESPACES, name = "app-controller")
@JBossLog
public class AppController extends BaseController<ApplicationCustomResource> {


    @Override
    protected UpdateControl<ApplicationCustomResource> patchResources(ApplicationCustomResource resource,
                                                                      Context<ApplicationCustomResource> context) {
        final boolean reschedule = handleJob(resource, false);
        return reschedule ? UpdateControl.updateStatus(resource)
                .rescheduleAfter(5, TimeUnit.SECONDS) : UpdateControl.updateStatus(resource);
    }

    @Override
    protected DeleteControl cleanupResources(ApplicationCustomResource resource,
                                             Context<ApplicationCustomResource> context) {
        final boolean reschedule = handleJob(resource, true);
        return reschedule ? DeleteControl.defaultDelete()
                .rescheduleAfter(5, TimeUnit.SECONDS) : DeleteControl.defaultDelete();
    }

    private boolean handleJob(ApplicationCustomResource application, boolean delete) {
        final String tenant = application.getSpec().getTenant();
        final String appId = application.getMetadata().getName();

        final ApplicationSpec spec = application.getSpec();
        log.infof("ApplicationSpec {}", spec);
        final String jobName;
        if (delete) {
            jobName = "sga-runtime-deployer-cleanup-" + appId;
        } else {
            jobName = "sga-runtime-deployer-" + appId;
        }
        final String targetNamespace = configuration.namespacePrefix() + tenant;
        final Job currentJob = client.batch().v1().jobs()
                .inNamespace(targetNamespace)
                .withName(jobName)
                .get();


        if (currentJob == null) {
            createJob(tenant, appId, spec, jobName, targetNamespace, delete);
            return true;
        } else {
            if (KubeUtil.isJobCompleted(currentJob)) {
                return false;
            } else {
                return true;
            }
        }
    }

    @SneakyThrows
    private void createJob(String tenant, String appId, ApplicationSpec spec, String jobName, String targetNamespace,
                           boolean delete) {

        // this is RuntimeDeployerConfiguration
        final Map<String, String> config = Map.of(
                "applicationId", appId,
                "application", spec.getApplication(),
                "tenant", tenant,
                "codeStorageArchiveId", spec.getCodeArchiveId() != null ? spec.getCodeArchiveId() : "");

        final Map<String, Object> clusterRuntime;
        if (configuration.clusterRuntime() == null) {
            clusterRuntime = Map.of();
        } else {
            clusterRuntime = SerializationUtil.readYaml(configuration.clusterRuntime(), Map.class);
        }
        final Map<String, Object> codeStorage;
        if (configuration.codeStorage() == null) {
            codeStorage = Map.of("type", "none");
        } else {
            codeStorage = SerializationUtil.readYaml(configuration.codeStorage(), Map.class);
        }

        final Container initContainer = new ContainerBuilder()
                .withName("deployer-init-config")
                .withImage(spec.getImage())
                .withImagePullPolicy(spec.getImagePullPolicy())
                .withCommand("bash", "-c")
                .withArgs("echo '%s' > /app-config/config && echo '%s' > /cluster-runtime-config/config".formatted(
                        SerializationUtil.writeAsJson(config),
                        SerializationUtil.writeAsJson(clusterRuntime)))
                .withVolumeMounts(new VolumeMountBuilder()
                                .withName("app-config")
                                .withMountPath("/app-config")
                                .build(),
                        new VolumeMountBuilder()
                                .withName("cluster-runtime-config")
                                .withMountPath("/cluster-runtime-config")
                                .build())
                .build();
        final String command = delete ? "delete" : "deploy";

        final Container container = new ContainerBuilder()
                .withName("deployer")
                .withImage(spec.getImage())
                .withImagePullPolicy(spec.getImagePullPolicy())
                .withArgs("deployer-runtime", command, "/cluster-runtime-config/config", "/app-config/config", "/app-secrets/secrets")
                .withVolumeMounts(new VolumeMountBuilder()
                                .withName("app-config")
                                .withMountPath("/app-config")
                                .build(),
                        new VolumeMountBuilder()
                                .withName("app-secrets")
                                .withMountPath("/app-secrets")
                                .build(),
                        new VolumeMountBuilder()
                                .withName("cluster-runtime-config")
                                .withMountPath("/cluster-runtime-config")
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
                .withServiceAccount(tenant)
                .withVolumes(new VolumeBuilder()
                                .withName("app-config")
                                .withEmptyDir(new EmptyDirVolumeSource())
                                .build(),
                        new VolumeBuilder()
                                .withName("app-secrets")
                                .withNewSecret()
                                .withSecretName(appId)
                                .endSecret()
                                .build(),
                        new VolumeBuilder()
                                .withName("cluster-runtime-config")
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
