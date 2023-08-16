/**
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
package com.datastax.oss.sga.deployer.k8s.controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.deployer.k8s.agents.AgentResourcesFactory;
import com.datastax.oss.sga.deployer.k8s.api.crds.agents.AgentCustomResource;
import com.datastax.oss.sga.deployer.k8s.util.SerializationUtil;
import com.datastax.oss.sga.api.model.AgentLifecycleStatus;
import com.datastax.oss.sga.runtime.api.agent.AgentSpec;
import com.datastax.oss.sga.runtime.api.agent.RuntimePodConfiguration;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetSpec;
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
    void testAgentController() throws Exception {
        final KubernetesClient client = deployment.getClient();
        final String namespace = "sga-my-tenant";
        client.resource(new NamespaceBuilder()
                .withNewMetadata()
                .withName(namespace)
                .endMetadata().build()).serverSideApply();

        final String agentCustomResourceName = AgentResourcesFactory.getAgentCustomResourceName("my-app", "agent-id");

        client
                .resource(AgentResourcesFactory.generateAgentSecret(agentCustomResourceName, new RuntimePodConfiguration(
                        Map.of("input", Map.of("is_input", true)),
                        Map.of("output", Map.of("is_output", true)),
                        new AgentSpec(AgentSpec.ComponentType.PROCESSOR, "my-tenant",
                                "agent-id", "my-app", "fn-type", Map.of("config", true), Map.of()),
                        new StreamingCluster("noop", Map.of("config", true))
                )))
                .inNamespace(namespace)
                .serverSideApply();

        final AgentCustomResource resource = getCr("""
                apiVersion: sga.oss.datastax.com/v1alpha1
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
                    tenant: my-tenant
                """.formatted(agentCustomResourceName, agentCustomResourceName));



        client.resource(resource).inNamespace(namespace).create();

        Awaitility.await().untilAsserted(() -> {
            assertEquals(1, client.apps().statefulSets().inNamespace(namespace).list().getItems().size());
            assertEquals(AgentLifecycleStatus.Status.DEPLOYING,
                    client.resource(resource).inNamespace(namespace).get().getStatus().getStatus().getStatus());
        });

        final StatefulSet statefulSet = client.apps().statefulSets().inNamespace(namespace).list().getItems().get(0);
        final StatefulSetSpec spec = statefulSet.getSpec();

        final PodSpec templateSpec = spec.getTemplate().getSpec();

        assertEquals(templateSpec.getVolumes().get(0).getName(), "app-config");
        assertEquals(templateSpec.getVolumes().get(0).getSecret().getSecretName(), agentCustomResourceName);
        assertEquals(templateSpec.getVolumes().get(0).getSecret().getItems().get(0).getKey(), "app-config");
        assertEquals(templateSpec.getVolumes().get(0).getSecret().getItems().get(0).getPath(), "config");
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
    private AgentCustomResource getCr(String yaml) {
        return SerializationUtil.readYaml(yaml, AgentCustomResource.class);
    }

}
