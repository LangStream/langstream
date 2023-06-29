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
package com.datastax.oss.sga.model.parser;

import com.datastax.oss.sga.model.AgentConfiguration;
import com.datastax.oss.sga.model.Application;
import com.datastax.oss.sga.model.Configuration;
import com.datastax.oss.sga.model.Connection;
import com.datastax.oss.sga.model.Module;
import com.datastax.oss.sga.model.Pipeline;
import com.datastax.oss.sga.model.TopicDefinition;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class ModelBuilder {

    static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    public static Application build(Path directory) throws Exception {
        log.info("Parsing directory: {}", directory.toAbsolutePath());

        Application application = new Application();
        try (DirectoryStream<Path> paths = Files.newDirectoryStream(directory);) {
            for (Path path :paths) {
                parseFile(path, application);
            }
        }
        return application;
    }

    private static void parseFile(Path path, Application application) throws IOException {
        String fileName = path.getFileName().toString();
        if (!fileName.endsWith(".yaml")) {
            // skip
            log.info("Skipping file {}", fileName);
            return;
        }

        switch (fileName) {
            case "configuration.yaml":
                parseConfiguration(path, application);
                break;
            case "secrets.yaml":
                parseSecrets(path, application);
                break;
            default:
                parsePipeline(path, application);
                break;
        }
    }

    private static void parseConfiguration(Path path, Application application) throws IOException {
        log.info("Reading Application Global Configuration from {}", path.toAbsolutePath());
        ConfigurationModel configuration = mapper.readValue(path.toFile(), ConfigurationModel.class);
        if (configuration.configuration() != null) {
            application.setConfiguration(configuration.configuration());
        }
        log.info("Configuration: {}", configuration);
    }

    private static void parsePipeline(Path path, Application application) throws IOException {
        log.info("Reading Pipeline from {}", path.toAbsolutePath());
        PipelineFileModel configuration = mapper.readValue(path.toFile(), PipelineFileModel.class);
        Module module = application.getModule(configuration.getModule());
        log.info("Configuration: {}", configuration);

        Pipeline pipeline = module.addPipeline(configuration.getId());
        pipeline.setName(configuration.getName());
        AgentConfiguration last = null;

        for (TopicDefinition topicDefinition : configuration.getTopics()) {
            module.addTopic(topicDefinition);
        }

        int autoId = 1;
        for (AgentModel agent: configuration.getPipeline()) {
            AgentConfiguration agentConfiguration = agent.toAgentConfiguration();
            if (agentConfiguration.getId() == null) {
                // ensure that we always have an id
                // please note that this algorithm should not be changed in order to not break
                // compatibility with existing configuration files
                agentConfiguration.setId(agentConfiguration.getType() + "_" + autoId++);
            }
            if (agent.getInput() != null) {
                agentConfiguration.setInput(new Connection(module.resolveTopic(agent.getInput())));
            }
            if (agent.getOutput() != null) {
                agentConfiguration.setOutput(new Connection(module.resolveTopic(agent.getOutput())));
            }
            if (last != null && agentConfiguration.getOutput() == null) {
                // assume that the previous agent is the output of this one
                agentConfiguration.setOutput(new Connection(last));
            }
            pipeline.addAgentConfiguration(agentConfiguration);
            last = agentConfiguration;
        }
    }

    private static void parseSecrets(Path path, Application application) throws IOException {
        log.info("Skipping Secrets from {}", path.toAbsolutePath());
    }


    @Data
    public static final class PipelineFileModel {
        private String module = Module.DEFAULT_MODULE;
        private String id;
        private String name;
        private List<AgentModel> pipeline = new ArrayList<>();

        private List<TopicDefinition> topics = new ArrayList<>();
    }

    @Data
    public static final class AgentModel {
        private String id;
        private String name;
        private String type;
        private String input;
        private String output;
        private Map<String, Object> configuration = new HashMap<>();

        AgentConfiguration toAgentConfiguration() {
            AgentConfiguration res =  new AgentConfiguration();
            res.setId(id);
            res.setName(name);
            res.setType(type);
            res.setConfiguration(configuration);
            return res;
        }
    }


    public record ConfigurationModel(Configuration configuration) {}
}
