package ai.langstream.runtime.impl.k8s.agents.vectors;

import ai.langstream.api.doc.AgentConfig;
import ai.langstream.api.doc.ConfigProperty;
import ai.langstream.runtime.impl.k8s.agents.QueryVectorDBAgentProvider;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import lombok.Data;

@Data
@AgentConfig(name = "Solr", description = """
    Writes data to Solr service.
    The collection-name is configured at datasource level.
""")
public class SolrVectorDatabaseSinkConfig extends QueryVectorDBAgentProvider.VectorDatabaseSinkConfig {

    public static final SolrVectorDatabaseSinkConfig INSTANCE = new SolrVectorDatabaseSinkConfig();

    @Override
    public Class getAgentConfigModelClass() {
        return SolrVectorDatabaseSinkConfig.class;
    }

    @Override
    public boolean isAgentConfigModelAllowUnknownProperties() {
        return false;
    }

    @Data
    public static class SolrField {

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
    List<SolrField> fields;
    @ConfigProperty(
            description = "Commit within option",
            defaultValue = "1000")
    @JsonProperty("commit-within")
    int commitWithin;
}