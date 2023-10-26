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
import lombok.Data;

@Data
public abstract class CassandraVectorDatabaseWriterConfig
        extends QueryVectorDBAgentProvider.VectorDatabaseWriterConfig {

    @AgentConfig(
            name = "Cassandra",
            description =
                    """
    Writes data to Apache Cassandra.
    All the options from DataStax Kafka Sink are supported: https://docs.datastax.com/en/kafka/doc/kafka/kafkaConfigTasksTOC.html
    """)
    public static class ApacheCassandraVectorDatabaseWriterConfig
            extends CassandraVectorDatabaseWriterConfig {
        @Override
        public Class getAgentConfigModelClass() {
            return ApacheCassandraVectorDatabaseWriterConfig.class;
        }
    }

    @AgentConfig(
            name = "Astra",
            description =
                    """
    Writes data to DataStax Astra service.
    All the options from DataStax Kafka Sink are supported: https://docs.datastax.com/en/kafka/doc/kafka/kafkaConfigTasksTOC.html
    """)
    public static class AstraVectorDatabaseWriterConfig
            extends CassandraVectorDatabaseWriterConfig {

        @Override
        public Class getAgentConfigModelClass() {
            return AstraVectorDatabaseWriterConfig.class;
        }
    }

    public static final ApacheCassandraVectorDatabaseWriterConfig CASSANDRA =
            new ApacheCassandraVectorDatabaseWriterConfig();
    public static final AstraVectorDatabaseWriterConfig ASTRA =
            new AstraVectorDatabaseWriterConfig();

    @Override
    public boolean isAgentConfigModelAllowUnknownProperties() {
        return true;
    }

    @ConfigProperty(
            description = "The name of the table to write to. The table must already exist.",
            required = true)
    @JsonProperty("table-name")
    String table;

    @ConfigProperty(description = "The keyspace of the table to write to.")
    String keyspace;

    @ConfigProperty(
            description =
                    "Comma separated list of mapping between the table column and the record field. e.g. my_colum_id=key, my_column_name=value.name.",
            required = true)
    String mapping;
}
