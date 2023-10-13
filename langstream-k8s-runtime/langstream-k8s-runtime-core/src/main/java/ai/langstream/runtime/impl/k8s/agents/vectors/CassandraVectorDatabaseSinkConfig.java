package ai.langstream.runtime.impl.k8s.agents.vectors;

import ai.langstream.api.doc.AgentConfig;
import ai.langstream.api.doc.ConfigProperty;
import ai.langstream.runtime.impl.k8s.agents.QueryVectorDBAgentProvider;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.Data;

@Data
@AgentConfig(name = "Cassandra/Astra", description = """
Writes data to Apache Cassandra or DataStax Astra service.
All the options from DataStax Kafka Sink are supported: https://docs.datastax.com/en/kafka/doc/kafka/kafkaConfigTasksTOC.html
""")
public class CassandraVectorDatabaseSinkConfig extends QueryVectorDBAgentProvider.VectorDatabaseSinkConfig {

    public static final CassandraVectorDatabaseSinkConfig INSTANCE = new CassandraVectorDatabaseSinkConfig();

    @Override
    public Class getAgentConfigModelClass() {
        return CassandraVectorDatabaseSinkConfig.class;
    }

    @Override
    public boolean isAgentConfigModelAllowUnknownProperties() {
        return true;
    }

    @ConfigProperty(
            description = "The name of the table to write to. The table must already exist.",
            required = true)
    @JsonProperty("table-name")
    String table;

    @ConfigProperty(
            description = "The keyspace of the table to write to.")
    String keyspace;
    @ConfigProperty(
            description = "Comma separated list of mapping between the table column and the record field. e.g. my_colum_id=key, my_column_name=value.name.",
            required = true)
    String mapping;
}
