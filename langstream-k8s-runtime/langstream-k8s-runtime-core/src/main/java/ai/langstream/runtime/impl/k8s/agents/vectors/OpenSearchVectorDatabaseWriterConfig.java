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
        name = "OpenSearch",
        description = "Writes data to OpenSearch or AWS OpenSearch serverless.")
public class OpenSearchVectorDatabaseWriterConfig
        extends QueryVectorDBAgentProvider.VectorDatabaseWriterConfig {

    public static final OpenSearchVectorDatabaseWriterConfig INSTANCE =
            new OpenSearchVectorDatabaseWriterConfig();

    @Override
    public Class getAgentConfigModelClass() {
        return OpenSearchVectorDatabaseWriterConfig.class;
    }

    @Override
    public boolean isAgentConfigModelAllowUnknownProperties() {
        return false;
    }

    @Data
    public static class IndexField {

        @ConfigProperty(description = "Field name", required = true)
        String name;

        @ConfigProperty(
                description = "JSTL Expression for computing the field value.",
                required = true)
        String expression;
    }

    @Data
    public static class BulkParameters {
        @ConfigProperty(
                description =
                        """
                The pipeline ID for preprocessing documents.
                Refer to the OpenSearch documentation for more details.
                """)
        String pipeline;

        @ConfigProperty(
                description =
                        """
                Whether to refresh the affected shards after performing the indexing operations. Default is false. true makes the changes show up in search results immediately, but hurts cluster performance. wait_for waits for a refresh. Requests take longer to return, but cluster performance doesn’t suffer.
                Note that AWS OpenSearch supports only false.
                Refer to the OpenSearch documentation for more details.
                """)
        String refresh;

        @ConfigProperty(
                description =
                        """
                Set to true to require that all actions target an index alias rather than an index.
                Refer to the OpenSearch documentation for more details.
                """)
        @JsonProperty("require_alias")
        Boolean requireAlias;

        @ConfigProperty(
                description =
                        """
                Routes the request to the specified shard.
                Refer to the OpenSearch documentation for more details.
                """)
        String routing;

        @ConfigProperty(
                description =
                        """
                How long to wait for the request to return.
                Refer to the OpenSearch documentation for more details.
                """)
        String timeout;

        @ConfigProperty(
                description =
                        """
                Specifies the number of active shards that must be available before OpenSearch processes the bulk request. Default is 1 (only the primary shard). Set to all or a positive integer. Values greater than 1 require replicas. For example, if you specify a value of 3, the index must have two replicas distributed across two additional nodes for the request to succeed.
                Refer to the OpenSearch documentation for more details.
                """)
        @JsonProperty("wait_for_active_shards")
        String waitForActiveShards;
    }

    @ConfigProperty(description = "Index fields definition.", required = true)
    List<IndexField> fields;

    @ConfigProperty(
            description =
                    "JSTL Expression to compute the index _id field. Leave it empty to let OpenSearch auto-generate the _id field.")
    String id;

    @ConfigProperty(description = "OpenSearch bulk URL parameters.")
    @JsonProperty("bulk-parameters")
    BulkParameters bulkParameters;

    @ConfigProperty(description = "Flush interval in milliseconds", defaultValue = "1000")
    @JsonProperty("flush-interval")
    int flushInterval;

    @ConfigProperty(
            description =
                    "Batch size for bulk operations. Hitting the batch size will trigger a flush.",
            defaultValue = "10")
    @JsonProperty("batch-size")
    int batchSize;
}
