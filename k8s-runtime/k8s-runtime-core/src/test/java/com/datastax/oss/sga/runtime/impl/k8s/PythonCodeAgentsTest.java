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
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class PythonCodeAgentsTest {

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
    public void testConfigurePythonAgents() throws Exception {
        Application applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("instance.yaml",
                        buildInstanceYaml(),
                        "module.yaml", """
                                module: "module-1"
                                id: "pipeline-1"                                
                                pipeline:
                                  - name: "source1"
                                    id: "source1"
                                    type: "python-source"
                                    configuration:                                                                      
                                      agent.class: my.python.module.MyClass                                 
                                      config1: value1
                                      config2: value2
                                  - name: "process1"
                                    id: "process1"
                                    type: "python-function"
                                    configuration:                                                                      
                                      agent.class: my.python.module.MyClass                                 
                                      config1: value1
                                      config2: value2
                                  - name: "sink1"
                                    id: "sink1"
                                    type: "python-sink"
                                    configuration:                                                                      
                                      agent.class: my.python.module.MyClass                                 
                                      config1: value1
                                      config2: value2
                                """));

        ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);
        assertEquals(3, implementation.getAgents().size());
        assertEquals(2, implementation.getTopics().size());

        {
            AgentNode agentImplementation = implementation.getAgentImplementation(module, "source1");
            assertNotNull(agentImplementation);
            DefaultAgentNode step =
                    (DefaultAgentNode) agentImplementation;
            Map<String, Object> configuration = step.getConfiguration();
            log.info("Configuration: {}", configuration);
            assertEquals("my.python.module.MyClass", configuration.get("agent.class"));
            assertEquals("value1", configuration.get("config1"));
            assertEquals("value2", configuration.get("config2"));
            assertEquals(ComponentType.SOURCE, step.getComponentType());
        }

        {
            AgentNode agentImplementation = implementation.getAgentImplementation(module, "process1");
            assertNotNull(agentImplementation);
            DefaultAgentNode step =
                    (DefaultAgentNode) agentImplementation;
            Map<String, Object> configuration = step.getConfiguration();
            log.info("Configuration: {}", configuration);
            assertEquals("my.python.module.MyClass", configuration.get("agent.class"));
            assertEquals("value1", configuration.get("config1"));
            assertEquals("value2", configuration.get("config2"));
            assertEquals(ComponentType.FUNCTION, step.getComponentType());
        }

        {
            AgentNode agentImplementation = implementation.getAgentImplementation(module, "sink1");
            assertNotNull(agentImplementation);
            DefaultAgentNode step =
                    (DefaultAgentNode) agentImplementation;
            Map<String, Object> configuration = step.getConfiguration();
            log.info("Configuration: {}", configuration);
            assertEquals("my.python.module.MyClass", configuration.get("agent.class"));
            assertEquals("value1", configuration.get("config1"));
            assertEquals("value2", configuration.get("config2"));
            assertEquals(ComponentType.SINK, step.getComponentType());
        }
    }


}