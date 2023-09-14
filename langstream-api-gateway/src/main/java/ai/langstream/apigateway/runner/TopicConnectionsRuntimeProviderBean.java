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
package ai.langstream.apigateway.runner;

import ai.langstream.api.runner.topics.TopicConnectionsRuntimeRegistry;
import ai.langstream.impl.nar.NarFileHandler;
import jakarta.annotation.PreDestroy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class TopicConnectionsRuntimeProviderBean {

    private final NarFileHandler narFileHandler;

    private final TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry;

    public TopicConnectionsRuntimeProviderBean(CodeConfiguration agentsConfiguration)
            throws Exception {

        log.info("Agents configuration: {}", agentsConfiguration);
        if (agentsConfiguration.getPath() != null) {
            Path directory = Paths.get(agentsConfiguration.getPath());

            if (Files.isDirectory(directory)) {
                log.info("Agents directory: {}", directory);
                this.narFileHandler =
                        new NarFileHandler(
                                directory, List.of(), NarFileHandler.class.getClassLoader());
                this.narFileHandler.scan();
            } else {
                log.info("Agents directory: {} does not exist", directory);
                this.narFileHandler = null;
            }
        } else {
            this.narFileHandler = null;
        }

        this.topicConnectionsRuntimeRegistry = new TopicConnectionsRuntimeRegistry();
        if (narFileHandler != null) {
            topicConnectionsRuntimeRegistry.setPackageLoader(narFileHandler);
        }
    }

    public TopicConnectionsRuntimeRegistry getTopicConnectionsRuntimeRegistry() {
        return topicConnectionsRuntimeRegistry;
    }

    @PreDestroy
    public void shutdown() {
        if (narFileHandler != null) {
            narFileHandler.close();
        }
    }
}
