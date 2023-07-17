package com.datastax.oss.sga.impl.deploy;

import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.ComputeCluster;
import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.api.runtime.ComputeClusterRuntime;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeRegistry;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.api.runtime.StreamingClusterRuntime;
import com.datastax.oss.sga.impl.noop.NoOpComputeClusterRuntimeProvider;
import com.datastax.oss.sga.impl.parser.ModelBuilder;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;

class ApplicationDeployerTest {


    static class MockClusterRuntimeRegistry extends ClusterRuntimeRegistry {
        public MockClusterRuntimeRegistry() {
            super();
        }

        public void addClusterRuntime(String name, ComputeClusterRuntime clusterRuntime) {
            computeClusterImplementations.put(name, clusterRuntime);
        }

        public void addStreamingClusterRuntime(String name, StreamingClusterRuntime clusterRuntime) {
            streamingClusterImplementations.put(name, clusterRuntime);
        }

        @Override
        public ComputeClusterRuntime getClusterRuntime(ComputeCluster computeCluster) {
            return computeClusterImplementations.get(computeCluster.type());
        }

        @Override
        public StreamingClusterRuntime getStreamingClusterRuntime(StreamingCluster streamingCluster) {
            return streamingClusterImplementations.get(streamingCluster.type());
        }
    }

    @Test
    void testDeploy() throws Exception {

        final MockClusterRuntimeRegistry registry = new MockClusterRuntimeRegistry();
        final ComputeClusterRuntime mockRuntime = spy(new NoOpComputeClusterRuntimeProvider.NoOpClusterRuntime());
        final StreamingClusterRuntime mockStreamingRuntime = Mockito.mock(StreamingClusterRuntime.class);
        registry.addClusterRuntime("mock", mockRuntime);
        registry.addStreamingClusterRuntime("mock", mockStreamingRuntime);

        final ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .pluginsRegistry(new PluginsRegistry())
                .registry(registry)
                .build();


        Application applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("configuration.yaml",
                        """
                                configuration:
                                    resources:
                                        - type: "openai-azure-config"
                                          name: "OpenAI Azure configuration"
                                          id: "openai-azure"
                                          configuration:
                                            credentials: "{{secrets.openai-credentials.accessKey}}"
                                    
                                """,
                        "secrets.yaml", """
                                secrets:
                                    - name: "OpenAI Azure credentials"
                                      id: "openai-credentials"
                                      data:
                                        accessKey: "my-access-key"
                                """,
                        "instance.yaml", """
                                instance:
                                    streamingCluster:
                                        type: mock
                                    computeCluster:
                                        type: mock
                                """));
        ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);
        deployer.deploy("tenant", implementation);
        Mockito.doAnswer(invocationOnMock -> {
            final Application resolvedApplicationInstance =
                    (Application) invocationOnMock.getArguments()[0];
            Assertions.assertEquals("my-access-key",
                    resolvedApplicationInstance.getResources().get("openai-azure").configuration()
                            .get("accessKey"));
            return null;
        }).when(mockRuntime).deploy(Mockito.anyString(), Mockito.any(), eq(mockStreamingRuntime) );
        Mockito.verify(mockRuntime).deploy(Mockito.anyString(), Mockito.any(), eq(mockStreamingRuntime));
    }
}