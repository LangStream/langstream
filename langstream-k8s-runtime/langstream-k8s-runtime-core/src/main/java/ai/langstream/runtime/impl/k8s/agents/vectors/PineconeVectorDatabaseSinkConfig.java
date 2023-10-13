package ai.langstream.runtime.impl.k8s.agents.vectors;

import ai.langstream.api.doc.AgentConfig;
import ai.langstream.api.doc.ConfigProperty;
import ai.langstream.runtime.impl.k8s.agents.QueryVectorDBAgentProvider;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;

@Data
@AgentConfig(name = "Pinecone", description = """
    Writes data to Pinecone service. 
""")
public class PineconeVectorDatabaseSinkConfig extends QueryVectorDBAgentProvider.VectorDatabaseSinkConfig {

    public static final PineconeVectorDatabaseSinkConfig INSTANCE = new PineconeVectorDatabaseSinkConfig();

    @Override
    public Class getAgentConfigModelClass() {
        return PineconeVectorDatabaseSinkConfig.class;
    }

    @Override
    public boolean isAgentConfigModelAllowUnknownProperties() {
        return false;
    }


    @ConfigProperty(
            description = "JSTL Expression to compute the id.")
    @JsonProperty("vector.id")
    String id;

    @ConfigProperty(
            description = "JSTL Expression to compute the vector.")
    @JsonProperty("vector.vector")
    String vector;

    @ConfigProperty(
            description = "JSTL Expression to compute the namespace.")
    @JsonProperty("vector.namespace")
    String namespace;

    @ConfigProperty(
            description = "Metadata to append. The key is the metadata name and the value the JSTL Expression to compute the actual value.")
    @JsonProperty("vector.metadata")
    Map<String, String> metadata;
}