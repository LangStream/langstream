package com.datastax.oss.sga.deployer.k8s.agents;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import com.datastax.oss.sga.api.model.AgentLifecycleStatus;
import com.datastax.oss.sga.api.model.ApplicationStatus;
import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.deployer.k8s.api.crds.agents.AgentCustomResource;
import com.datastax.oss.sga.deployer.k8s.util.SerializationUtil;
import com.datastax.oss.sga.impl.k8s.tests.KubeK3sServer;
import com.datastax.oss.sga.runtime.k8s.api.PodAgentConfiguration;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class AgentAggregatedStatusTest {


    @RegisterExtension
    static final KubeK3sServer k3s = new KubeK3sServer(true);

    @Test
    void testAggregatePodStatus() {
        final String tenant = "my-tenant";
        final String namespace = "sga-%s".formatted(tenant);
        final String agentId = "agent-id";
        final String applicationId = "my-app";
        deployAgent(tenant, namespace, agentId, applicationId);
        Awaitility.await().atMost(1, TimeUnit.MINUTES).untilAsserted(() -> {
            final Map<String, ApplicationStatus.AgentStatus> status =
                    AgentResourcesFactory.aggregateAgentsStatus(k3s.getClient(), namespace, applicationId,
                            List.of(agentId));
            System.out.println("got status" + status);
            assertEquals(1, status.size());
            final ApplicationStatus.AgentStatus agentStatus = status.get(agentId);
            assertEquals(AgentLifecycleStatus.Status.CREATED, agentStatus.getStatus().getStatus());
            assertEquals(1, agentStatus.getWorkers().size());
            final ApplicationStatus.AgentWorkerStatus workerStatus =
                    agentStatus.getWorkers().get("my-app-agent-id-0");
            assertEquals(ApplicationStatus.AgentWorkerStatus.Status.ERROR, workerStatus.getStatus());
            assertEquals("failed to create containerd task: failed to create shim task: OCI runtime create failed: "
                    + "runc create failed: unable to start container process: exec: \"bash\": executable file not "
                    + "found in $PATH: unknown", workerStatus.getReason());

        });
    }

    private void deployAgent(String tenant, String namespace, String agentId, String applicationId) {
        k3s.getClient().resource(new NamespaceBuilder()
                        .withNewMetadata().withName(namespace).endMetadata().build())
                .serverSideApply();
        k3s.getClient().resource(new ServiceAccountBuilder()
                        .withNewMetadata().withName(tenant).endMetadata().build())
                .inNamespace(namespace)
                .serverSideApply();

        final PodAgentConfiguration podConf = new PodAgentConfiguration(
                Map.of("input", Map.of("is_input", true)),
                Map.of("output", Map.of("is_output", true)),
                new PodAgentConfiguration.AgentConfiguration(agentId, "my-agent", "FUNCTION", Map.of("config", true)),
                new StreamingCluster("noop", Map.of("config", true)),
                new PodAgentConfiguration.CodeStorageConfiguration("code-storage-id")
        );
        final AgentCustomResource resource = getCr("""
                apiVersion: sga.oss.datastax.com/v1alpha1
                kind: Agent
                metadata:
                  name: %s
                spec:
                    image: busybox
                    imagePullPolicy: IfNotPresent
                    configuration: '%s'
                    tenant: %s
                    applicationId: %s
                """.formatted(applicationId + "-" + agentId, SerializationUtil.writeAsJson(podConf), tenant,
                applicationId));
        resource.getMetadata().setLabels(AgentResourcesFactory.getAgentLabels(agentId, applicationId));
        k3s.getClient().resource(resource).inNamespace(namespace).serverSideApply();
        final StatefulSet statefulSet = AgentResourcesFactory.generateStatefulSet(resource, Map.of());
        k3s.getClient().resource(statefulSet).inNamespace(namespace).serverSideApply();
    }

    private AgentCustomResource getCr(String yaml) {
        return SerializationUtil.readYaml(yaml, AgentCustomResource.class);
    }
}