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
package ai.langstream.tests.util.kafka;

import ai.langstream.api.model.StreamingCluster;
import ai.langstream.tests.util.BaseEndToEndTest;
import ai.langstream.tests.util.StreamingClusterProvider;
import ai.langstream.tests.util.SystemOrEnv;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;

@Slf4j
public class RemoteKafkaProvider implements StreamingClusterProvider {

    private static final String SYS_PROPERTIES_PREFIX = "langstream.tests.remotekafka.props.";
    private static final String ENV_PREFIX = "LANGSTREAM_TESTS_REMOTEKAFKA_PROPS_";
    private static final Map<String, Object> ADMIN_CONFIG;

    private org.apache.kafka.clients.admin.Admin admin;

    static {
        ADMIN_CONFIG = new HashMap<>();
        final Map<String, String> props =
                SystemOrEnv.getProperties(ENV_PREFIX, SYS_PROPERTIES_PREFIX);
        props.forEach(
                (key, value) -> {
                    final String newKey = key.replace("_", ".");
                    ADMIN_CONFIG.put(newKey, value);
                    log.info("Loading remote kafka admin config: {}={}", newKey, value);
                });
    }

    @Override
    public StreamingCluster start() {
        admin = Admin.create(ADMIN_CONFIG);
        final StreamingCluster streamingCluster =
                new StreamingCluster("kafka", Map.of("admin", ADMIN_CONFIG));
        cleanup();
        return streamingCluster;
    }

    @Override
    @SneakyThrows
    public void cleanup() {
        final List<String> topics = getTopics();
        if (!topics.isEmpty()) {
            log.info("Deleting topics: {}", topics);
            admin.deleteTopics(topics).all().get();
            log.info("Deleted topics: {}", topics);
        }
        log.info("Topics after cleanup: {}", getTopics());
    }

    @Override
    public void stop() {
        if (admin != null) {
            admin.close();
        }
    }

    @Override
    @SneakyThrows
    public List<String> getTopics() {
        return admin.listTopics().names().get().stream()
                .filter(s -> s.startsWith(BaseEndToEndTest.TOPICS_PREFIX))
                .collect(Collectors.toList());
    }
}
