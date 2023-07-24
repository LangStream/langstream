package com.datastax.oss.sga.impl.storage.k8s.apps;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.Secrets;
import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.api.storage.ApplicationStore;
import com.datastax.oss.sga.deployer.k8s.agents.AgentResourceUnitConfiguration;
import com.datastax.oss.sga.deployer.k8s.agents.AgentResourcesFactory;
import com.datastax.oss.sga.deployer.k8s.api.crds.agents.AgentCustomResource;
import com.datastax.oss.sga.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import com.datastax.oss.sga.deployer.k8s.apps.AppResourcesFactory;
import com.datastax.oss.sga.deployer.k8s.util.SerializationUtil;
import com.datastax.oss.sga.impl.k8s.tests.KubeK3sServer;
import com.datastax.oss.sga.runtime.api.agent.AgentSpec;
import com.datastax.oss.sga.runtime.api.agent.CodeStorageConfig;
import com.datastax.oss.sga.runtime.api.agent.RuntimePodConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class KubernetesApplicationStoreLogsTest {

    @RegisterExtension
    static final KubeK3sServer k3s = new KubeK3sServer(true);

    @Test
    void testLogs() {
        final KubernetesApplicationStore store = new KubernetesApplicationStore();
        store.initialize(Map.of("namespaceprefix", "sga-", "deployer-runtime", Map.of("image", "busybox", "image-pull-policy", "IfNotPresent")));
        store.onTenantCreated("mytenant");
        AgentCustomResource cr = agentCustomResource("mytenant", "myapp");
        k3s.getClient().resource(cr).inNamespace("sga-mytenant").serverSideApply();
        cr = k3s.getClient().resource(cr).inNamespace("sga-mytenant").get();
        final StatefulSet sts = AgentResourcesFactory.generateStatefulSet(
                cr,
                Map.of(),
                new AgentResourceUnitConfiguration()
        );
        sts.getSpec().getTemplate().getSpec().getInitContainers().clear();
        sts.getSpec().getTemplate().getSpec().getContainers().get(0)
                        .setCommand(List.of("sh", "-c"));
        sts.getSpec().getTemplate().getSpec().getContainers().get(0)
                .setArgs(List.of("while true; do echo 'hello'; sleep 1000000; done"));
        k3s.getClient().resource(sts).inNamespace("sga-mytenant").serverSideApply();

        Awaitility.await().untilAsserted(() -> {
            final List<ApplicationStore.PodLogHandler> podHandlers =
                    store.logs("mytenant", "myapp", new ApplicationStore.LogOptions());
            assertEquals(2, podHandlers.size());
        });
        List<ApplicationStore.PodLogHandler> podHandlers =
                store.logs("mytenant", "myapp", new ApplicationStore.LogOptions());
        podHandlers.get(0)
                .start(new ApplicationStore.LogLineConsumer() {
                    @Override
                    public boolean onLogLine(String line) {
                        assertEquals("\u001B[32mmyapp-agent111-0 hello\u001B[0m\n", line);
                        return false;
                    }
                });
        podHandlers.get(1)
                .start(new ApplicationStore.LogLineConsumer() {
                    @Override
                    public boolean onLogLine(String line) {
                        assertEquals("\u001B[33mmyapp-agent111-1 hello\u001B[0m\n", line);
                        return false;
                    }
                });

        podHandlers = store.logs("mytenant", "myapp",
                new ApplicationStore.LogOptions(List.of("myapp-agent111-1")));
        assertEquals(1, podHandlers.size());
        podHandlers.get(0)
                .start(new ApplicationStore.LogLineConsumer() {
                    @Override
                    public boolean onLogLine(String line) {
                        assertEquals("\u001B[33mmyapp-agent111-1 hello\u001B[0m\n", line);
                        return false;
                    }
                });

    }

    private static AgentCustomResource agentCustomResource(final String tenant, final String applicationId) {

        final String agentId = "agent111";
        final String agentCustomResourceName = AgentResourcesFactory.getAgentCustomResourceName("my-app", "agent-id");

        k3s.getClient()
                .resource(AgentResourcesFactory.generateAgentSecret(agentCustomResourceName, new RuntimePodConfiguration(
                        Map.of("input", Map.of("is_input", true)),
                        Map.of("output", Map.of("is_output", true)),
                        new AgentSpec(AgentSpec.ComponentType.FUNCTION, "my-tenant",
                                "agent-id", "my-app", "fn-type", Map.of("config", true)),
                        new StreamingCluster("noop", Map.of("config", true)),
                        new CodeStorageConfig("none", "", Map.of())

                )))
                .inNamespace("sga-" + tenant)
                .serverSideApply();

        return SerializationUtil.readYaml("""
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
                    resources:
                        size: 1
                        parallelism: 2
                """.formatted(agentCustomResourceName, tenant, applicationId, agentId, agentCustomResourceName), AgentCustomResource.class);
    }
}