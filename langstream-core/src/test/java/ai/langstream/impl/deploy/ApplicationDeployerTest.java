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
package ai.langstream.impl.deploy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;

import ai.langstream.api.model.Application;
import ai.langstream.api.model.ComputeCluster;
import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.runner.topics.TopicConnectionsRuntime;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeAndLoader;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeRegistry;
import ai.langstream.api.runtime.*;
import ai.langstream.impl.noop.NoOpComputeClusterRuntimeProvider;
import ai.langstream.impl.parser.ModelBuilder;
import java.util.Map;
import lombok.Cleanup;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ApplicationDeployerTest {

    static class MockClusterRuntimeRegistry extends ClusterRuntimeRegistry {
        public MockClusterRuntimeRegistry() {
            super();
        }

        public void addClusterRuntime(String name, ComputeClusterRuntime clusterRuntime) {
            computeClusterImplementations.put(name, clusterRuntime);
        }

        public void addStreamingClusterRuntime(
                String name, StreamingClusterRuntime clusterRuntime) {
            streamingClusterImplementations.put(name, clusterRuntime);
        }

        @Override
        public ComputeClusterRuntime getClusterRuntime(ComputeCluster computeCluster) {
            return computeClusterImplementations.get(computeCluster.type());
        }

        @Override
        public StreamingClusterRuntime getStreamingClusterRuntime(
                StreamingCluster streamingCluster) {
            return streamingClusterImplementations.get(streamingCluster.type());
        }
    }

    @Test
    void testDeploy() throws Exception {

        final MockClusterRuntimeRegistry registry = new MockClusterRuntimeRegistry();
        final ComputeClusterRuntime mockRuntime =
                spy(new NoOpComputeClusterRuntimeProvider.NoOpClusterRuntime());
        final StreamingClusterRuntime mockStreamingRuntime =
                Mockito.mock(StreamingClusterRuntime.class);
        final TopicConnectionsRuntime mockTopicConnectionsRuntime =
                Mockito.mock(TopicConnectionsRuntime.class);
        registry.addClusterRuntime("mock", mockRuntime);
        registry.addStreamingClusterRuntime("mock", mockStreamingRuntime);

        final @Cleanup ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .pluginsRegistry(new PluginsRegistry())
                        .registry(registry)
                        .deployContext(DeployContext.NO_DEPLOY_CONTEXT)
                        .topicConnectionsRuntimeRegistry(
                                new TopicConnectionsRuntimeRegistry() {
                                    @Override
                                    public TopicConnectionsRuntimeAndLoader
                                            getTopicConnectionsRuntime(
                                                    StreamingCluster streamingCluster) {
                                        assertEquals(streamingCluster.type(), "mock");
                                        return new TopicConnectionsRuntimeAndLoader(
                                                mockTopicConnectionsRuntime,
                                                Thread.currentThread().getContextClassLoader());
                                    }
                                })
                        .build();

        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "configuration.yaml",
                                        """
                                configuration:
                                    resources:
                                        - type: "openai-azure-config"
                                          name: "OpenAI Azure configuration"
                                          id: "openai-azure"
                                          configuration:
                                            credentials: "${secrets.openai-credentials.accessKey}"

                                """),
                                """
                                instance:
                                    streamingCluster:
                                        type: mock
                                    computeCluster:
                                        type: mock
                                """,
                                """
                                 secrets:
                                    - name: "OpenAI Azure credentials"
                                      id: "openai-credentials"
                                      data:
                                        accessKey: "my-access-key"
                                """)
                        .getApplication();
        ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);
        deployer.deploy("tenant", implementation, null);
        Mockito.doAnswer(
                        invocationOnMock -> {
                            final Application resolvedApplicationInstance =
                                    (Application) invocationOnMock.getArguments()[0];
                            assertEquals(
                                    "my-access-key",
                                    resolvedApplicationInstance
                                            .getResources()
                                            .get("openai-azure")
                                            .configuration()
                                            .get("accessKey"));
                            return null;
                        })
                .when(mockRuntime)
                .deploy(
                        Mockito.anyString(),
                        Mockito.any(),
                        eq(mockStreamingRuntime),
                        Mockito.any(),
                        Mockito.any());
        Mockito.verify(mockRuntime)
                .deploy(
                        Mockito.anyString(),
                        Mockito.any(),
                        eq(mockStreamingRuntime),
                        Mockito.any(),
                        Mockito.any());
    }
}
