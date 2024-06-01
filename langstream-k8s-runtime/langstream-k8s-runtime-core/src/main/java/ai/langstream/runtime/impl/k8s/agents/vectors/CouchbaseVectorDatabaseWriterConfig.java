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
@AgentConfig(
        name = "Couchbase Vector DB",
        description =
                """
Writes data to Couchbase Capella.
All the options from DataStax Kafka Sink are supported: https://docs.datastax.com/en/kafka/doc/kafka/kafkaConfigTasksTOC.html
""")
public class CouchbaseVectorDatabaseWriterConfig
        extends QueryVectorDBAgentProvider.VectorDatabaseWriterConfig {

    public static final QueryVectorDBAgentProvider.VectorDatabaseWriterConfig INSTANCE =
            new CouchbaseVectorDatabaseWriterConfig();

    @Override
    public Class getAgentConfigModelClass() {
        return CouchbaseVectorDatabaseWriterConfig.class;
    }

    @Override
    public boolean isAgentConfigModelAllowUnknownProperties() {
        return true;
    }

    @ConfigProperty(
            description = "The connection string to the Couchbase cluster.",
            required = true)
    @JsonProperty("connection-string")
    private String connectionString;

    @ConfigProperty(description = "The name of the bucket to write to.", required = true)
    @JsonProperty("bucket-name")
    private String bucketName;

    @ConfigProperty(
            description = "The username to connect to the Couchbase cluster.",
            required = true)
    @JsonProperty("username")
    private String username;

    @ConfigProperty(
            description = "The password to connect to the Couchbase cluster.",
            required = true)
    @JsonProperty("password")
    private String password;
}
