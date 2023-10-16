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
@AgentConfig(name = "JDBC", description = """
    Writes data to any JDBC compatible database.
""")
public class JDBCVectorDatabaseWriterConfig
        extends QueryVectorDBAgentProvider.VectorDatabaseWriterConfig {

    public static final JDBCVectorDatabaseWriterConfig INSTANCE =
            new JDBCVectorDatabaseWriterConfig();

    @Override
    public Class getAgentConfigModelClass() {
        return JDBCVectorDatabaseWriterConfig.class;
    }

    @Override
    public boolean isAgentConfigModelAllowUnknownProperties() {
        return false;
    }

    @Data
    public static class TableField {

        @ConfigProperty(
                description = "Is this field part of the primary key?",
                defaultValue = "false")
        @JsonProperty("primary-key")
        boolean primaryKey;

        @ConfigProperty(description = "Field name", required = true)
        String name;

        @ConfigProperty(
                description = "JSTL Expression for computing the field value.",
                required = true)
        String expression;
    }

    @ConfigProperty(
            description = "The name of the table to write to. The table must already exist.",
            required = true)
    @JsonProperty("table-name")
    String table;

    @ConfigProperty(description = "Fields of the table to write to.", required = true)
    List<TableField> fields;
}
