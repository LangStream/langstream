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
import java.util.Map;
import lombok.Data;

@Data
@AgentConfig(
        name = "Pinecone",
        description =
                """
    Writes data to Pinecone service.
    To add metadata fields you can add vector.metadata.my-field: "value.my-field". The value is a JSTL Expression to compute the actual value.
""")
public class PineconeVectorDatabaseWriterConfig
        extends QueryVectorDBAgentProvider.VectorDatabaseWriterConfig {

    public static final PineconeVectorDatabaseWriterConfig INSTANCE =
            new PineconeVectorDatabaseWriterConfig();

    @Override
    public Class getAgentConfigModelClass() {
        return PineconeVectorDatabaseWriterConfig.class;
    }

    @Override
    public boolean isAgentConfigModelAllowUnknownProperties() {
        return true;
    }

    @ConfigProperty(description = "JSTL Expression to compute the id.")
    @JsonProperty("vector.id")
    String id;

    @ConfigProperty(description = "JSTL Expression to compute the vector.")
    @JsonProperty("vector.vector")
    String vector;

    @ConfigProperty(description = "JSTL Expression to compute the namespace.")
    @JsonProperty("vector.namespace")
    String namespace;

    @ConfigProperty(
            description =
                    "Metadata to append. The key is the metadata name and the value the JSTL Expression to compute the actual value.")
    @JsonProperty("vector.metadata")
    Map<String, String> metadata;
}
