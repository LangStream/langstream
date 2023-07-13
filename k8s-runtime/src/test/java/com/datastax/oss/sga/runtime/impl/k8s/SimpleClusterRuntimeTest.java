package com.datastax.oss.sga.runtime.impl.k8s;

import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.Connection;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.TopicDefinition;
import com.datastax.oss.sga.api.runtime.AgentNode;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeRegistry;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.impl.deploy.ApplicationDeployer;
import com.datastax.oss.sga.impl.noop.NoOpStreamingClusterRuntimeProvider;
import com.datastax.oss.sga.impl.parser.ModelBuilder;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class SimpleClusterRuntimeTest {

    @NotNull
    private static String buildInstanceYaml() {
        return """
                instance:
                  streamingCluster:
                    type: "noop"                    
                  computeCluster:
                    type: "kubernetes"                    
                """;
    }

    @Test
    public void testMapGenericPulsarFunctionsChain() throws Exception {
        Application applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("instance.yaml",
                        buildInstanceYaml(),
                        "module.yaml", """
                                module: "module-1"
                                id: "pipeline-1"                                
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                  - name: "output-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "generic-agent"
                                    id: "function-1-id"
                                    type: "generic-agent"
                                    input: "input-topic"
                                    # the output is implicitly an intermediate topic                                    
                                    configuration:
                                      config1: "value"
                                      config2: "value2"
                                  - name: "function2"
                                    id: "function-2-id"
                                    type: "generic-agent"
                                    # the input is implicitly an intermediate topic                                    
                                    output: "output-topic"
                                    configuration:
                                      config1: "value"
                                      config2: "value2"
                                """));

        ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation(applicationInstance);
        {
            assertTrue(implementation.getConnectionImplementation(module,
                    new Connection(TopicDefinition.fromName("input-topic"))) instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
            assertTrue(implementation.getTopics().values().stream().anyMatch( t-> ((NoOpStreamingClusterRuntimeProvider.SimpleTopic) t).name().equals("input-topic")));
        }
        {
            assertTrue(implementation.getConnectionImplementation(module,
                    new Connection(TopicDefinition.fromName("output-topic"))) instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
            assertTrue(implementation.getTopics().values().stream().anyMatch( t-> ((NoOpStreamingClusterRuntimeProvider.SimpleTopic) t).name().equals("output-topic")));
        }

        {
            assertTrue(implementation.getConnectionImplementation(module, new Connection(
                    TopicDefinition.fromName("agent-function-1-id-output"))) instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
            assertTrue(implementation.getTopics().values().stream().anyMatch( t-> ((NoOpStreamingClusterRuntimeProvider.SimpleTopic) t).name().equals("agent-function-1-id-output")));
        }


        assertEquals(3, implementation.getTopics().size());

        {
            AgentNode agentImplementation = implementation.getAgentImplementation(module, "function-1-id");
            assertNotNull(agentImplementation);
            assertEquals("generic-agent", agentImplementation.getAgentType());
            assertEquals("function-1-id", agentImplementation.getId());
        }

        {
            AgentNode agentImplementation = implementation.getAgentImplementation(module, "function-2-id");
            assertNotNull(agentImplementation);
            assertEquals("generic-agent", agentImplementation.getAgentType());
            assertEquals("function-2-id", agentImplementation.getId());
        }

    }

}