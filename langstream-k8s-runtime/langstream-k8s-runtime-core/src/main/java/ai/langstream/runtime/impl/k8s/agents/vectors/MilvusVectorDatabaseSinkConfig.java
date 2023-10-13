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
public class MilvusVectorDatabaseSinkConfig extends QueryVectorDBAgentProvider.VectorDatabaseSinkConfig {

    public static final MilvusVectorDatabaseSinkConfig INSTANCE = new MilvusVectorDatabaseSinkConfig();

    @Override
    public Class getAgentConfigModelClass() {
        return MilvusVectorDatabaseSinkConfig.class;
    }

    @Override
    public boolean isAgentConfigModelAllowUnknownProperties() {
        return false;
    }

    @Data
    public static class MilvusField {

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
            description = "Fields definition.",
            required = true)
    List<MilvusField> fields;

    @ConfigProperty(
            description = "Collection name")
    @JsonProperty("collection-name")
    String collectionName;

    @ConfigProperty(
            description = "Collection name")
    @JsonProperty("database-name")
    String databaseName;
}