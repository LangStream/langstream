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
        name = "Apache Solr",
        description =
                """
    Writes data to Apache Solr service.
    The collection-name is configured at datasource level.
""")
public class SolrVectorDatabaseWriterConfig
        extends QueryVectorDBAgentProvider.VectorDatabaseWriterConfig {

    public static final SolrVectorDatabaseWriterConfig INSTANCE =
            new SolrVectorDatabaseWriterConfig();

    @Override
    public Class getAgentConfigModelClass() {
        return SolrVectorDatabaseWriterConfig.class;
    }

    @Override
    public boolean isAgentConfigModelAllowUnknownProperties() {
        return false;
    }

    @Data
    public static class SolrField {

        @ConfigProperty(description = "Field name", required = true)
        String name;

        @ConfigProperty(
                description = "JSTL Expression for computing the field value.",
                required = true)
        String expression;
    }

    @ConfigProperty(description = "Fields definition.", required = true)
    List<SolrField> fields;

    @ConfigProperty(description = "Commit within option", defaultValue = "1000")
    @JsonProperty("commit-within")
    int commitWithin;
}
