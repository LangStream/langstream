package com.datastax.oss.sga.deployer.k8s.controllers;

import com.datastax.oss.sga.deployer.k8s.DeployerConfiguration;
import com.datastax.oss.sga.deployer.k8s.api.crds.agents.AgentCustomResource;
import com.datastax.oss.sga.deployer.k8s.api.crds.agents.AgentSpec;
import com.datastax.oss.sga.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import com.datastax.oss.sga.deployer.k8s.api.crds.apps.ApplicationSpec;
import com.datastax.oss.sga.deployer.k8s.util.KubeUtil;
import com.datastax.oss.sga.deployer.k8s.util.SerializationUtil;
import com.datastax.oss.sga.runtime.agent.PodJavaRuntime;
import com.datastax.oss.sga.runtime.agent.RuntimePodConfiguration;
import com.datastax.oss.sga.runtime.impl.k8s.PodAgentConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.svm.core.annotate.Delete;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EmptyDirVolumeSource;
import io.fabric8.kubernetes.api.model.EnvFromSourceBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Constants;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import lombok.extern.jbosslog.JBossLog;

@ControllerConfiguration(namespaces = Constants.WATCH_ALL_NAMESPACES, name = "agent-controller")
@JBossLog
public class AgentController extends BaseController<AgentCustomResource> {

    @Override
    protected UpdateControl<AgentCustomResource> patchResources(AgentCustomResource agent,
                                                                      Context<AgentCustomResource> context) {
        final boolean reschedule = handleResource(agent);
        return reschedule ? UpdateControl.updateStatus(agent)
                .rescheduleAfter(5, TimeUnit.SECONDS) : UpdateControl.updateStatus(agent);
    }

    @Override
    protected DeleteControl cleanupResources(AgentCustomResource resource,
                                             Context<AgentCustomResource> context) {
        return DeleteControl.defaultDelete();
    }

    private boolean handleResource(AgentCustomResource agent) {
        final String tenant = agent.getSpec().getTenant();
        final String agentName = agent.getMetadata().getName();

        final AgentSpec spec = agent.getSpec();
        final String stsName = agentName;
        final String targetNamespace = configuration.namespacePrefix() + tenant;
        final StatefulSet current = client.apps().statefulSets()
                .inNamespace(targetNamespace)
                .withName(stsName)
                .get();


        if (current == null) {
            final StatefulSet sts = generateStatefulSet(agent);
            KubeUtil.patchStatefulSet(client, sts);
            return true;
        } else {
            if (KubeUtil.isStatefulSetReady(current)) {
                return false;
            } else {
                return true;
            }
        }
    }


    public StatefulSet generateStatefulSet(AgentCustomResource agentCustomResource) {

        final AgentSpec spec = agentCustomResource.getSpec();
        final PodAgentConfiguration podAgentConfiguration =
                SerializationUtil.readJson(spec.getConfiguration(), PodAgentConfiguration.class);
        RuntimePodConfiguration podConfig = new RuntimePodConfiguration(
                podAgentConfiguration.input(),
                podAgentConfiguration.output(),
                new com.datastax.oss.sga.runtime.agent.AgentSpec(
                        com.datastax.oss.sga.runtime.agent.AgentSpec.ComponentType.valueOf(podAgentConfiguration.agentConfiguration().componentType()),
                        podAgentConfiguration.agentConfiguration().agentType(),
                        podAgentConfiguration.agentConfiguration().configuration()
                ),
                podAgentConfiguration.streamingCluster()
        );



        final Container container = new ContainerBuilder()
                .withName("runtime")
                .withImage(spec.getImage())
                .withImagePullPolicy(spec.getImagePullPolicy())
//                .withLivenessProbe(createLivenessProbe())
//                .withReadinessProbe(createReadinessProbe())
                // .withResources(spec.getResources())
                .withArgs("agent-runtime", "/app-config/config")
                .withVolumeMounts(new VolumeMountBuilder()
                        .withName("app-config")
                        .withMountPath("/app-config")
                        .build()
                )
                .build();

        final Container initContainer = new ContainerBuilder()
                .withName("runtime-init-config")
                .withImage(spec.getImage())
                .withImagePullPolicy(spec.getImagePullPolicy())
                .withCommand("bash", "-c")
                .withArgs("echo '%s' > /app-config/config".formatted(SerializationUtil.writeAsJson(podConfig)))
                .withVolumeMounts(new VolumeMountBuilder()
                                .withName("app-config")
                                .withMountPath("/app-config")
                                .build())
                .build();

        final String tenant = spec.getTenant();
        final StatefulSet statefulSet = new StatefulSetBuilder()
                .withNewMetadata()
                .withName(agentCustomResource.getMetadata().getName())
                .withNamespace(agentCustomResource.getMetadata().getNamespace())
                .withLabels(Map.of("app", "sga-runtime", "tenant", tenant))
                .endMetadata()
                .withNewSpec()
                // TODO: replicas
                .withReplicas(1)
                .withNewSelector()
                .withMatchLabels(Map.of("app", "sga-runtime", "tenant", tenant))
                .endSelector()
                .withPodManagementPolicy("Parallel")
                .withNewTemplate()
                .withNewMetadata()
                .withLabels(Map.of("app", "sga-runtime", "tenant", tenant))
                .endMetadata()
                .withNewSpec()
                .withServiceAccountName(tenant)
                .withTerminationGracePeriodSeconds(60L)
                .withInitContainers(List.of(initContainer))
                .withContainers(List.of(container))
                .withVolumes(new VolumeBuilder()
                        .withName("app-config")
                        .withEmptyDir(new EmptyDirVolumeSource())
                        .build()
                )
                .endSpec()
                .endTemplate()
                .endSpec()
                .build();
        return statefulSet;
    }

}
