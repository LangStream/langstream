package com.datastax.oss.sga.runtime.impl.k8s;

import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.Connection;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.TopicDefinition;
import com.datastax.oss.sga.api.runtime.AgentNode;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeRegistry;
import com.datastax.oss.sga.api.runtime.ComponentType;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.impl.common.DefaultAgentNode;
import com.datastax.oss.sga.impl.deploy.ApplicationDeployer;
import com.datastax.oss.sga.impl.noop.NoOpStreamingClusterRuntimeProvider;
import com.datastax.oss.sga.impl.parser.ModelBuilder;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class KafkaConnectAgentsTest {

    @NotNull
    private static String buildInstanceYaml() {
        return """
                instance:
                  streamingCluster:
                    type: "noop"                    
                  computeCluster:
                    type: "none"                    
                """;
    }

    @Test
    public void testConfigureKafkaConnectSink() throws Exception {
        Application applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("instance.yaml",
                        buildInstanceYaml(),
                        "module.yaml", """
                                module: "module-1"
                                id: "pipeline-1"                                
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "sink1"
                                    id: "step1"
                                    type: "sink"
                                    input: "input-topic"
                                    configuration:                                                                      
                                      connector.class: FileStreamSink                                      
                                      file: /tmp/test.sink.txt
                                """));

        @Cleanup ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);
        assertEquals(1, implementation.getAgents().size());
        assertTrue(implementation.getConnectionImplementation(module,
                new Connection(TopicDefinition.fromName("input-topic"))) instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);

        AgentNode agentImplementation = implementation.getAgentImplementation(module, "step1");
        assertNotNull(agentImplementation);
        DefaultAgentNode step =
                (DefaultAgentNode) agentImplementation;
        Map<String, Object> configuration = step.getConfiguration();
        log.info("Configuration: {}", configuration);
        assertEquals("FileStreamSink", configuration.get("connector.class"));
        assertEquals("input-topic", configuration.get("topics"));
        assertEquals("/tmp/test.sink.txt", configuration.get("file"));
        assertEquals(ComponentType.SINK, step.getComponentType());
    }

    @Test
    public void testConfigureKafkaConnectSources() throws Exception {
        Application applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("instance.yaml",
                        buildInstanceYaml(),
                        "module.yaml", """
                                module: "module-1"
                                id: "pipeline-1"                                
                                topics:
                                  - name: "output-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "source1"
                                    id: "step1"
                                    type: "source"
                                    output: "output-topic"
                                    configuration:                                                                      
                                      connector.class: FileStreamSource                                                              
                                      file: /tmp/test.txt
                                """));

        @Cleanup ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);
        assertEquals(1, implementation.getAgents().size());
        assertTrue(implementation.getConnectionImplementation(module,
                new Connection(TopicDefinition.fromName("output-topic"))) instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);

        AgentNode agentImplementation = implementation.getAgentImplementation(module, "step1");
        assertNotNull(agentImplementation);
        DefaultAgentNode step =
                (DefaultAgentNode) agentImplementation;
        Map<String, Object> configuration = step.getConfiguration();
        log.info("Configuration: {}", configuration);
        assertEquals("FileStreamSource", configuration.get("connector.class"));
        assertEquals("output-topic", configuration.get("topic"));
        assertEquals("/tmp/test.txt", configuration.get("file"));
        assertEquals(ComponentType.SOURCE, step.getComponentType());
    }

}