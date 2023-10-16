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
@AgentConfig(name = "Milvus", description = """
    Writes data to Milvus/Zillis service.
""")
public class MilvusVectorDatabaseWriterConfig
        extends QueryVectorDBAgentProvider.VectorDatabaseWriterConfig {

    public static final MilvusVectorDatabaseWriterConfig INSTANCE =
            new MilvusVectorDatabaseWriterConfig();

    @Override
    public Class getAgentConfigModelClass() {
        return MilvusVectorDatabaseWriterConfig.class;
    }

    @Override
    public boolean isAgentConfigModelAllowUnknownProperties() {
        return false;
    }

    @Data
    public static class MilvusField {

        @ConfigProperty(description = "Field name", required = true)
        String name;

        @ConfigProperty(
                description = "JSTL Expression for computing the field value.",
                required = true)
        String expression;
    }

    @ConfigProperty(description = "Fields definition.", required = true)
    List<MilvusField> fields;

    @ConfigProperty(description = "Collection name")
    @JsonProperty("collection-name")
    String collectionName;

    @ConfigProperty(description = "Collection name")
    @JsonProperty("database-name")
    String databaseName;
}
