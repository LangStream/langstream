package com.datastax.oss.sga.impl.k8s.tests;

import com.datastax.oss.sga.deployer.k8s.api.crds.agents.AgentCustomResource;
import com.datastax.oss.sga.impl.k8s.KubernetesClientFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class KubeTestServer implements AutoCloseable, BeforeEachCallback, BeforeAllCallback, AfterAllCallback {

    public static class Server extends KubernetesMockServer {

        @Override
        @SneakyThrows
        public void init() {
            super.init();
            KubernetesClientFactory.set(null, createClient());
        }

        @Override
        public void destroy() {
            super.destroy();
            KubernetesClientFactory.clear();
        }
    }

    private final Server server = new Server();

    @SneakyThrows
    public void start() {
        server.init();
    }

    @Override
    public void close() throws Exception {
        server.destroy();
    }

    @Override
    public void beforeEach(ExtensionContext extensionContext) throws Exception {
        server.reset();
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) throws Exception {
        close();
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        start();
    }

    public Map<String, AgentCustomResource> spyAgentCustomResources(final String namespace,
                                                                    String... expectedAgents) {


        Map<String, AgentCustomResource> agents = new HashMap<>();
        for (String agentId : expectedAgents) {
            final String fullPath =
                    "/apis/sga.oss.datastax.com/v1alpha1/namespaces/%s/agents/%s?fieldManager=fabric8".formatted(
                            namespace, agentId);
            System.out.println("mocking: " + fullPath);
            server.expect()
                    .patch()
                    .withPath(fullPath)
                    .andReply(
                            HttpURLConnection.HTTP_OK,
                            recordedRequest -> {
                                try {
                                    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                                    recordedRequest.getBody().copyTo(byteArrayOutputStream);
                                    final ObjectMapper mapper = new ObjectMapper();
                                    final AgentCustomResource agent =
                                            mapper.readValue(byteArrayOutputStream.toByteArray(),
                                                    AgentCustomResource.class);
                                    agents.put(agent.getMetadata().getName(), agent);
                                    return agent;
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                    ).always();
        }
        return agents;
    }

}
