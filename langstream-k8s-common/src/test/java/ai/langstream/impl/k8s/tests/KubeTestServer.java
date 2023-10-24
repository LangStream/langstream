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
package ai.langstream.impl.k8s.tests;

import static org.mockito.ArgumentMatchers.isNull;

import ai.langstream.deployer.k8s.api.crds.agents.AgentCustomResource;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import ai.langstream.impl.k8s.KubernetesClientFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

@Slf4j
public class KubeTestServer
        implements AutoCloseable, BeforeEachCallback, BeforeAllCallback, AfterAllCallback {

    public static class Server extends KubernetesMockServer {

        MockedStatic<KubernetesClientFactory> mocked;
        NamespacedKubernetesClient client;

        @Override
        @SneakyThrows
        public void init() {
            super.init();

            mocked = Mockito.mockStatic(KubernetesClientFactory.class);
            client = createClient();

            mocked.when(() -> KubernetesClientFactory.get(isNull())).thenReturn(client);
            mocked.when(() -> KubernetesClientFactory.create(isNull())).thenReturn(client);
        }

        @Override
        public void destroy() {
            super.destroy();
            if (mocked != null) {
                mocked.close();
            }
            if (client != null) {
                client.close();
            }
        }
    }

    @Getter private Server server;

    @SneakyThrows
    public void start() {
        server = new Server();
        server.init();
    }

    @Override
    public void close() throws Exception {
        if (server != null) {
            server.destroy();
            server = null;
        }
    }

    @Override
    public void beforeEach(ExtensionContext extensionContext) {
        server.reset();
        currentAgents.clear();
        currentAgentsSecrets.clear();
        currentApplications.clear();
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) {
        start();
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) throws Exception {
        close();
    }

    private final Map<String, AgentCustomResource> currentAgents = new HashMap<>();
    private final Map<String, ApplicationCustomResource> currentApplications = new HashMap<>();
    private final Map<String, Secret> currentAgentsSecrets = new HashMap<>();

    private static final String CRD_VERSION = "v1alpha1";

    public Map<String, AgentCustomResource> spyAgentCustomResources(
            final String namespace, String... expectedAgents) {
        for (String agentId : expectedAgents) {
            final String fullPath =
                    "/apis/langstream.ai/%s/namespaces/%s/agents/%s"
                            .formatted(CRD_VERSION, namespace, agentId);
            server.expect()
                    .patch()
                    .withPath(fullPath + "?fieldManager=fabric8")
                    .andReply(
                            HttpURLConnection.HTTP_OK,
                            recordedRequest -> {
                                try {
                                    final ByteArrayOutputStream byteArrayOutputStream =
                                            new ByteArrayOutputStream();
                                    recordedRequest.getBody().copyTo(byteArrayOutputStream);
                                    final ObjectMapper mapper =
                                            new ObjectMapper()
                                                    .enable(
                                                            SerializationFeature
                                                                    .ORDER_MAP_ENTRIES_BY_KEYS);
                                    final AgentCustomResource agent =
                                            mapper.readValue(
                                                    byteArrayOutputStream.toByteArray(),
                                                    AgentCustomResource.class);
                                    log.info("received patch request for agent {}", agentId);
                                    currentAgents.put(agentId, agent);
                                    return agent;
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            })
                    .always();

            server.expect()
                    .get()
                    .withPath(fullPath)
                    .andReply(
                            HttpURLConnection.HTTP_OK,
                            recordedRequest -> {
                                log.info("received get request for agent {}", agentId);
                                return currentAgents.get(agentId);
                            })
                    .always();

            server.expect()
                    .delete()
                    .withPath(fullPath)
                    .andReply(
                            HttpURLConnection.HTTP_OK,
                            recordedRequest -> {
                                log.info("received delete request for agent {}", agentId);
                                currentAgents.remove(agentId);
                                return List.of();
                            })
                    .always();
        }
        return currentAgents;
    }

    public Map<String, Secret> spyAgentCustomResourcesSecrets(
            final String namespace, String... expectedAgents) {
        for (String agentId : expectedAgents) {
            final String fullPath =
                    "/api/v1/namespaces/%s/secrets/%s".formatted(namespace, agentId);
            server.expect()
                    .patch()
                    .withPath(fullPath + "?fieldManager=fabric8")
                    .andReply(
                            HttpURLConnection.HTTP_OK,
                            recordedRequest -> {
                                try {
                                    final ByteArrayOutputStream byteArrayOutputStream =
                                            new ByteArrayOutputStream();
                                    recordedRequest.getBody().copyTo(byteArrayOutputStream);
                                    final ObjectMapper mapper = new ObjectMapper();
                                    final Secret secret =
                                            mapper.readValue(
                                                    byteArrayOutputStream.toByteArray(),
                                                    Secret.class);
                                    log.info(
                                            "received patch request secret for agent {}: {}",
                                            agentId,
                                            secret);
                                    currentAgentsSecrets.put(agentId, secret);
                                    return secret;
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            })
                    .always();

            server.expect()
                    .delete()
                    .withPath(fullPath)
                    .andReply(
                            HttpURLConnection.HTTP_OK,
                            recordedRequest -> {
                                log.info("received delete request for secret agent {}", agentId);
                                currentAgentsSecrets.remove(agentId);
                                return List.of();
                            })
                    .always();
        }
        return currentAgentsSecrets;
    }

    public void expectTenantCreated(String tenant) {
        final String namespace = "langstream-" + tenant;

        server.expect()
                .patch()
                .withPath("/api/v1/namespaces/%s?fieldManager=fabric8".formatted(namespace))
                .andReply(HttpURLConnection.HTTP_OK, recordedRequest -> null)
                .always();

        server.expect()
                .get()
                .withPath("/api/v1/namespaces/%s".formatted(namespace))
                .andReply(
                        HttpURLConnection.HTTP_OK,
                        recordedRequest ->
                                new NamespaceBuilder()
                                        .withNewMetadata()
                                        .withName(namespace)
                                        .endMetadata()
                                        .build())
                .always();

        server.expect()
                .patch()
                .withPath(
                        "/api/v1/namespaces/%s/serviceaccounts/%s?fieldManager=fabric8"
                                .formatted(namespace, tenant))
                .andReply(HttpURLConnection.HTTP_OK, recordedRequest -> null)
                .always();

        server.expect()
                .patch()
                .withPath(
                        "/api/v1/namespaces/%s/serviceaccounts/runtime-%s?fieldManager=fabric8"
                                .formatted(namespace, tenant))
                .andReply(HttpURLConnection.HTTP_OK, recordedRequest -> null)
                .always();
        server.expect()
                .patch()
                .withPath(
                        "/apis/rbac.authorization.k8s.io/v1/namespaces/%s/roles/%s?fieldManager=fabric8"
                                .formatted(namespace, tenant))
                .andReply(HttpURLConnection.HTTP_OK, recordedRequest -> null)
                .always();

        server.expect()
                .patch()
                .withPath(
                        "/apis/rbac.authorization.k8s.io/v1/namespaces/%s/rolebindings/%s?fieldManager=fabric8"
                                .formatted(namespace, tenant))
                .andReply(HttpURLConnection.HTTP_OK, recordedRequest -> null)
                .always();

        server.expect()
                .patch()
                .withPath(
                        "/api/v1/namespaces/%s/secrets/langstream-cluster-config?fieldManager=fabric8"
                                .formatted(namespace))
                .andReply(HttpURLConnection.HTTP_OK, recordedRequest -> null)
                .always();
    }

    public Map<String, ApplicationCustomResource> spyApplicationCustomResources(
            String namespace, String... applicationIds) {
        for (String appId : applicationIds) {
            final String fullPath =
                    "/apis/langstream.ai/%s/namespaces/%s/applications/%s"
                            .formatted(CRD_VERSION, namespace, appId);
            server.expect()
                    .patch()
                    .withPath(fullPath + "?fieldManager=fabric8")
                    .andReply(
                            HttpURLConnection.HTTP_OK,
                            recordedRequest -> {
                                try {
                                    final ByteArrayOutputStream byteArrayOutputStream =
                                            new ByteArrayOutputStream();
                                    recordedRequest.getBody().copyTo(byteArrayOutputStream);
                                    final ObjectMapper mapper =
                                            new ObjectMapper()
                                                    .enable(
                                                            SerializationFeature
                                                                    .ORDER_MAP_ENTRIES_BY_KEYS);
                                    final ApplicationCustomResource app =
                                            mapper.readValue(
                                                    byteArrayOutputStream.toByteArray(),
                                                    ApplicationCustomResource.class);
                                    log.info("received patch request for app {}", appId);
                                    currentApplications.put(appId, app);
                                    return app;
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            })
                    .always();

            server.expect()
                    .patch()
                    .withPath(
                            "/api/v1/namespaces/%s/secrets/%s?fieldManager=fabric8"
                                    .formatted(namespace, appId))
                    .andReply(HttpURLConnection.HTTP_OK, recordedRequest -> null)
                    .always();

            server.expect()
                    .get()
                    .withPath(fullPath)
                    .andReply(
                            HttpURLConnection.HTTP_OK,
                            recordedRequest -> {
                                return currentApplications.get(appId);
                            })
                    .always();

            server.expect()
                    .delete()
                    .withPath(fullPath)
                    .andReply(
                            HttpURLConnection.HTTP_OK,
                            recordedRequest -> {
                                log.info("received delete request for app {}", appId);
                                currentApplications.remove(appId);
                                return List.of();
                            })
                    .always();
        }
        return currentApplications;
    }
}
