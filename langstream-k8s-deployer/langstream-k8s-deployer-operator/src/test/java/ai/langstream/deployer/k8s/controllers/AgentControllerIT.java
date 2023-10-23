/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.langstream.deployer.k8s.controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import ai.langstream.api.model.AgentLifecycleStatus;
import ai.langstream.api.model.StreamingCluster;
import ai.langstream.deployer.k8s.agents.AgentResourcesFactory;
import ai.langstream.deployer.k8s.api.crds.agents.AgentCustomResource;
import ai.langstream.deployer.k8s.util.SerializationUtil;
import ai.langstream.runtime.api.agent.AgentSpec;
import ai.langstream.runtime.api.agent.RuntimePodConfiguration;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetSpec;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@Testcontainers
public class AgentControllerIT {

    static final Map<String, String> DEPLOYER_CONFIG =
            new HashMap<>(
                    Map.of(
                            "DEPLOYER_RUNTIME_IMAGE", "busybox",
                            "DEPLOYER_RUNTIME_IMAGE_PULL_POLICY", "IfNotPresent"));

    @RegisterExtension
    static final OperatorExtension deployment = new OperatorExtension(DEPLOYER_CONFIG);

    @Test
    void testAgentController() {
        final KubernetesClient client = deployment.getClient();
        final String tenant = genTenant();
        final String namespace = "langstream-" + tenant;
        createNamespace(client, namespace);

        final String agentCustomResourceName =
                AgentResourcesFactory.getAgentCustomResourceName("my-app", "agent-id");

        createAgentSecret(client, tenant, agentCustomResourceName, namespace);

        final AgentCustomResource resource =
                getCr(
                        """
                apiVersion: langstream.ai/v1beta1
                kind: Agent
                metadata:
                  name: %s
                spec:
                    applicationId: my-app
                    agentId: agent-id
                    agentConfigSecretRef: %s
                    agentConfigSecretRefChecksum: xx
                    tenant: %s
                """
                                .formatted(
                                        agentCustomResourceName, agentCustomResourceName, tenant));

        client.resource(resource).inNamespace(namespace).create();

        Awaitility.await()
                .untilAsserted(
                        () -> {
                            assertEquals(
                                    1,
                                    client.apps()
                                            .statefulSets()
                                            .inNamespace(namespace)
                                            .list()
                                            .getItems()
                                            .size());
                            assertEquals(
                                    AgentLifecycleStatus.Status.DEPLOYING,
                                    client.resource(resource)
                                            .inNamespace(namespace)
                                            .get()
                                            .getStatus()
                                            .getStatus()
                                            .getStatus());
                        });

        final StatefulSet statefulSet =
                client.apps().statefulSets().inNamespace(namespace).list().getItems().get(0);
        final StatefulSetSpec spec = statefulSet.getSpec();

        final PodSpec templateSpec = spec.getTemplate().getSpec();

        assertEquals(templateSpec.getVolumes().get(0).getName(), "app-config");
        assertEquals(
                templateSpec.getVolumes().get(0).getSecret().getSecretName(),
                agentCustomResourceName);
        assertEquals(
                templateSpec.getVolumes().get(0).getSecret().getItems().get(0).getKey(),
                "app-config");
        assertEquals(
                templateSpec.getVolumes().get(0).getSecret().getItems().get(0).getPath(), "config");
        final Container container = templateSpec.getContainers().get(0);
        assertEquals("busybox", container.getImage());
        assertEquals("IfNotPresent", container.getImagePullPolicy());
        assertEquals("runtime", container.getName());
        assertEquals("/app-config", container.getVolumeMounts().get(0).getMountPath());
        assertEquals("app-config", container.getVolumeMounts().get(0).getName());
        assertEquals(0, container.getCommand().size());
        int args = 0;
        assertEquals("agent-runtime", container.getArgs().get(args++));
        assertEquals("/app-config/config", container.getArgs().get(args++));
    }

    static AtomicInteger counter = new AtomicInteger(0);

    private String genTenant() {
        return "my-tenant-" + counter.incrementAndGet();
    }

    private AgentCustomResource getCr(String yaml) {
        return SerializationUtil.readYaml(yaml, AgentCustomResource.class);
    }

    @Test
    void testDoNotUpdateStatefulsetIfDeployerRestarted() {
        final KubernetesClient client = deployment.getClient();
        final String tenant = genTenant();
        final String namespace = "langstream-" + tenant;
        createNamespace(client, namespace);

        final String agentCustomResourceName =
                AgentResourcesFactory.getAgentCustomResourceName("my-app", "agent-id");

        createAgentSecret(client, tenant, agentCustomResourceName, namespace);

        AgentCustomResource resource =
                getCr(
                        """
                apiVersion: langstream.ai/v1beta1
                kind: Agent
                metadata:
                  name: %s
                spec:
                    image: busybox
                    imagePullPolicy: IfNotPresent
                    applicationId: my-app
                    agentId: agent-id
                    agentConfigSecretRef: %s
                    agentConfigSecretRefChecksum: xx
                    tenant: %s
                """
                                .formatted(
                                        agentCustomResourceName, agentCustomResourceName, tenant));

        client.resource(resource).inNamespace(namespace).create();

        Awaitility.await()
                .untilAsserted(
                        () -> {
                            assertEquals(
                                    1,
                                    client.apps()
                                            .statefulSets()
                                            .inNamespace(namespace)
                                            .list()
                                            .getItems()
                                            .size());
                            assertEquals(
                                    AgentLifecycleStatus.Status.DEPLOYING,
                                    client.resources(AgentCustomResource.class)
                                            .inNamespace(namespace)
                                            .withName(agentCustomResourceName)
                                            .get()
                                            .getStatus()
                                            .getStatus()
                                            .getStatus());
                        });
        resource = client.resource(resource).inNamespace(namespace).get();
        assertNotNull(resource.getStatus().getLastConfigApplied());

        StatefulSet statefulSet =
                client.apps().statefulSets().inNamespace(namespace).list().getItems().get(0);
        assertEquals(
                "busybox",
                statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());

        DEPLOYER_CONFIG.put("DEPLOYER_RUNTIME_IMAGE", "busybox:v2");
        try {
            deployment.restartDeployerOperator();

            resource = client.resource(resource).inNamespace(namespace).get();
            resource.getSpec().setCodeArchiveId("another");
            client.resource(resource).inNamespace(namespace).serverSideApply();

            Awaitility.await()
                    .atMost(1, TimeUnit.MINUTES)
                    .untilAsserted(
                            () -> {
                                final String archiveId =
                                        SerializationUtil.readJson(
                                                        client.resources(AgentCustomResource.class)
                                                                .inNamespace(namespace)
                                                                .withName(agentCustomResourceName)
                                                                .get()
                                                                .getStatus()
                                                                .getLastApplied(),
                                                        ai.langstream.deployer.k8s.api.crds.agents
                                                                .AgentSpec.class)
                                                .getCodeArchiveId();
                                assertEquals("another", archiveId);
                            });
            statefulSet =
                    client.apps().statefulSets().inNamespace(namespace).list().getItems().get(0);
            assertEquals(
                    "busybox",
                    statefulSet
                            .getSpec()
                            .getTemplate()
                            .getSpec()
                            .getContainers()
                            .get(0)
                            .getImage());
        } finally {
            DEPLOYER_CONFIG.put("DEPLOYER_RUNTIME_IMAGE", "busybox");
        }
    }

    private void createAgentSecret(
            KubernetesClient client,
            String tenant,
            String agentCustomResourceName,
            String namespace) {
        client.resource(
                        AgentResourcesFactory.generateAgentSecret(
                                agentCustomResourceName,
                                new RuntimePodConfiguration(
                                        Map.of("input", Map.of("is_input", true)),
                                        Map.of("output", Map.of("is_output", true)),
                                        new AgentSpec(
                                                AgentSpec.ComponentType.PROCESSOR,
                                                tenant,
                                                "agent-id",
                                                "my-app",
                                                "fn-type",
                                                Map.of("config", true),
                                                Map.of(),
                                                Set.of()),
                                        new StreamingCluster("noop", Map.of("config", true)))))
                .inNamespace(namespace)
                .serverSideApply();
    }

    private void createNamespace(KubernetesClient client, String namespace) {
        client.resource(
                        new NamespaceBuilder()
                                .withNewMetadata()
                                .withName(namespace)
                                .endMetadata()
                                .build())
                .serverSideApply();
    }
}
