package com.datastax.oss.sga.deployer.k8s.controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.deployer.k8s.api.crds.agents.AgentCustomResource;
import com.datastax.oss.sga.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import com.datastax.oss.sga.deployer.k8s.util.SerializationUtil;
import com.datastax.oss.sga.runtime.agent.AgentSpec;
import com.datastax.oss.sga.runtime.impl.k8s.PodAgentConfiguration;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetSpec;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobSpec;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@Testcontainers
public class AgentControllerIT {

    @RegisterExtension
    static final OperatorExtension deployment = new OperatorExtension();

    @Test
    void testAppController() throws Exception {


        final PodAgentConfiguration podConf = new PodAgentConfiguration(
                Map.of("input", Map.of("is_input", true)),
                Map.of("output", Map.of("is_output", true)),
                new PodAgentConfiguration.AgentConfiguration("agent-id", "my-agent", "FUNCTION", Map.of("config", true)),
                new StreamingCluster("noop", Map.of("config", true))
        );


        final AgentCustomResource resource = getCr("""
                apiVersion: sga.oss.datastax.com/v1alpha1
                kind: Agent
                metadata:
                  name: test-agent1
                  namespace: default
                spec:
                    image: ubuntu
                    imagePullPolicy: Always
                    configuration: '%s'
                    tenant: my-tenant
                    applicationId: the-app
                """.formatted(SerializationUtil.writeAsJson(podConf)));
        final KubernetesClient client = deployment.getClient();
        final String namespace = "sga-my-tenant";
        client.resource(new NamespaceBuilder()
                .withNewMetadata()
                .withName(namespace)
                .endMetadata().build()).serverSideApply();
        client.resource(resource).inNamespace(namespace).create();

        Awaitility.await().untilAsserted(() -> {
            assertEquals(1, client.apps().statefulSets().inNamespace(namespace).list().getItems().size());
        });
        final StatefulSet statefulSet = client.apps().statefulSets().inNamespace(namespace).list().getItems().get(0);
        final StatefulSetSpec spec = statefulSet.getSpec();

        assertEquals(spec.getTemplate().getMetadata().getLabels().get("app"), "sga-runtime");
        assertEquals(spec.getTemplate().getMetadata().getLabels().get("tenant"), "my-tenant");

        final PodSpec templateSpec = spec.getTemplate().getSpec();
        final Container container = templateSpec.getContainers().get(0);
        assertEquals("ubuntu", container.getImage());
        assertEquals("Always", container.getImagePullPolicy());
        assertEquals("runtime", container.getName());
        assertEquals("/app-config", container.getVolumeMounts().get(0).getMountPath());
        assertEquals("app-config", container.getVolumeMounts().get(0).getName());
        assertEquals(0, container.getCommand().size());
        int args = 0;
        assertEquals("agent-runtime", container.getArgs().get(args++));
        assertEquals("/app-config/config", container.getArgs().get(args++));

        final Container initContainer = templateSpec.getInitContainers().get(0);
        assertEquals("ubuntu", initContainer.getImage());
        assertEquals("Always", initContainer.getImagePullPolicy());
        assertEquals("runtime-init-config", initContainer.getName());
        assertEquals("/app-config", initContainer.getVolumeMounts().get(0).getMountPath());
        assertEquals("app-config", initContainer.getVolumeMounts().get(0).getName());
        assertEquals("bash", initContainer.getCommand().get(0));
        assertEquals("-c", initContainer.getCommand().get(1));
        assertEquals("echo '{\"input\":{\"input\":{\"is_input\":true}},\"output\":{\"output\":{\"is_output\":true}},"
                + "\"agent\":{\"componentType\":\"FUNCTION\",\"agentId\":\"agent-id\",\"applicationId\":\"the-app\",\"agentType\":\"my-agent\","
                + "\"configuration\":{\"config\":true}},\"streamingCluster\":{\"type\":\"noop\","
                + "\"configuration\":{\"config\":true}}}' > /app-config/config", initContainer.getArgs().get(0));

    }
    private AgentCustomResource getCr(String yaml) {
        return SerializationUtil.readYaml(yaml, AgentCustomResource.class);
    }

}