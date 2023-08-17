/**
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
package ai.langstream.pulsar;

import com.datastax.oss.sga.api.model.SchemaDefinition;
import com.datastax.oss.sga.api.runtime.ConnectionImplementation;
import com.datastax.oss.sga.api.runtime.Topic;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public record PulsarTopic(PulsarName name, int partitions,  SchemaDefinition keySchema, SchemaDefinition valueSchema, String createMode, boolean implicit)
        implements ConnectionImplementation, Topic {

    @Override
    public String topicName() {
        return name.toPulsarName();
    }

    @Override
    public boolean implicit() {
        return this.implicit;
    }

    @Override
    public void bindDeadletterTopic(Topic deadletterTopic) {
        log.error("Error deadletter topic configuration on Pulsar: {}", deadletterTopic);
    }
}
