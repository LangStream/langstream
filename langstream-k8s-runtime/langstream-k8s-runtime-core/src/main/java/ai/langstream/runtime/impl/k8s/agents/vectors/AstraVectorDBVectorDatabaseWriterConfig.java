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
package ai.langstream.runtime.impl.k8s.agents.vectors;

import ai.langstream.api.doc.AgentConfig;
import ai.langstream.api.doc.ConfigProperty;
import ai.langstream.runtime.impl.k8s.agents.QueryVectorDBAgentProvider;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.Data;

@Data
@AgentConfig(
        name = "Astra Vector DB",
        description =
                """
Writes data to Apache Cassandra.
All the options from DataStax Kafka Sink are supported: https://docs.datastax.com/en/kafka/doc/kafka/kafkaConfigTasksTOC.html
""")
public class AstraVectorDBVectorDatabaseWriterConfig
        extends QueryVectorDBAgentProvider.VectorDatabaseWriterConfig {

    public static final QueryVectorDBAgentProvider.VectorDatabaseWriterConfig INSTANCE =
            new AstraVectorDBVectorDatabaseWriterConfig();

    @Override
    public Class getAgentConfigModelClass() {
        return AstraVectorDBVectorDatabaseWriterConfig.class;
    }

    @Override
    public boolean isAgentConfigModelAllowUnknownProperties() {
        return true;
    }

    @ConfigProperty(
            description =
                    "The name of the collection to write to. The collection must already exist.",
            required = true)
    @JsonProperty("collection-name")
    String collectionName;

    @Data
    public static class CollectionField {

        @ConfigProperty(description = "Field name", required = true)
        String name;

        @ConfigProperty(
                description = "JSTL Expression for computing the field value.",
                required = true)
        String expression;
    }

    @ConfigProperty(description = "Fields of the collection to write to.", required = true)
    List<CollectionField> fields;
}
