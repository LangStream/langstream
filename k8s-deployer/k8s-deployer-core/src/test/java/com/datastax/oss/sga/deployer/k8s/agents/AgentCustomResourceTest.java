package com.datastax.oss.sga.deployer.k8s.agents;

import static org.junit.jupiter.api.Assertions.assertEquals;
import com.datastax.oss.sga.api.model.AgentLifecycleStatus;
import com.datastax.oss.sga.api.model.ApplicationStatus;
import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.deployer.k8s.api.crds.agents.AgentCustomResource;
import com.datastax.oss.sga.deployer.k8s.api.crds.agents.AgentSpec;
import com.datastax.oss.sga.deployer.k8s.util.SerializationUtil;
import com.datastax.oss.sga.impl.k8s.tests.KubeK3sServer;
import com.datastax.oss.sga.runtime.api.agent.RuntimePodConfiguration;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;

class AgentCustomResourceTest {


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

    @Test
    void testStatefulsetBeingDeleted() {
        deployAgent("tenant", "sga-tenant", "my-agent", "my-app2");
        assertEquals(1, k3s.getClient().apps().statefulSets().inNamespace("sga-tenant")
                .list().getItems().size());

        k3s.getClient().resources(AgentCustomResource.class)
                .inNamespace("sga-tenant")
                .withName("my-app2-my-agent")
                .delete();

        Awaitility.await()
                .untilAsserted(() -> {
                    assertEquals(0, k3s.getClient().apps().statefulSets().inNamespace("sga-tenant")
                            .list().getItems().size());
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

        final String name = AgentResourcesFactory.getAgentCustomResourceName(applicationId, agentId);

        k3s.getClient()
                .resource(AgentResourcesFactory.generateAgentSecret(name, Mockito.mock(RuntimePodConfiguration.class)))
                .inNamespace(namespace)
                .serverSideApply();
        AgentCustomResource resource = getCr("""
                apiVersion: sga.oss.datastax.com/v1alpha1
                kind: Agent
                metadata:
                  name: %s
                spec:
                    tenant: %s
                    applicationId: %s
                    agentId: %s
                    image: "busybox"
                    imagePullPolicy: IfNotPresent
                    agentConfigSecretRef: %s
                    agentConfigSecretRefChecksum: xx
                """.formatted(name, tenant, applicationId, agentId, name));
        resource.getMetadata().setLabels(AgentResourcesFactory.getAgentLabels(agentId, applicationId));
        k3s.getClient().resource(resource).inNamespace(namespace).serverSideApply();
        resource = k3s.getClient().resource(resource).inNamespace(namespace).get();
        final StatefulSet statefulSet = AgentResourcesFactory.generateStatefulSet(resource, Map.of(),
                new AgentResourceUnitConfiguration());
        k3s.getClient().resource(statefulSet).inNamespace(namespace).serverSideApply();
    }

    private AgentCustomResource getCr(String yaml) {
        return SerializationUtil.readYaml(yaml, AgentCustomResource.class);
    }
}