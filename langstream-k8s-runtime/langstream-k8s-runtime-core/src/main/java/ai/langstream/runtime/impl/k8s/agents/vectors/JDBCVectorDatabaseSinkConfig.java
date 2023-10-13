package ai.langstream.runtime.impl.k8s.agents.vectors;

import ai.langstream.api.doc.AgentConfig;
import ai.langstream.api.doc.ConfigProperty;
import ai.langstream.api.util.ConfigurationUtils;
import ai.langstream.runtime.impl.k8s.agents.QueryVectorDBAgentProvider;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import lombok.Data;

@Data
@AgentConfig(name = "JDBC", description = """
    Writes data to any JDBC compatible database. 
""")
public class JDBCVectorDatabaseSinkConfig extends QueryVectorDBAgentProvider.VectorDatabaseSinkConfig {

    public static final JDBCVectorDatabaseSinkConfig INSTANCE = new JDBCVectorDatabaseSinkConfig();

    @Override
    public Class getAgentConfigModelClass() {
        return JDBCVectorDatabaseSinkConfig.class;
    }

    @Override
    public boolean isAgentConfigModelAllowUnknownProperties() {
        return false;
    }


    @Data
    public static class TableField {

        @ConfigProperty(description = "Is this field part of the primary key?", defaultValue = "false")
        @JsonProperty("primary-key")
        boolean primaryKey;


        @ConfigProperty(
                description = "Field name",
                required = true)
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

    @ConfigProperty(
            description = "Fields of the table to write to.",
            required = true
    )
    List<TableField> fields;
}