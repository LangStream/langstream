package ai.langstream.agents.vector.datasource.impl;

import ai.langstream.agents.vector.VectorDBSinkAgent;
import ai.langstream.api.runner.code.AgentCodeRegistry;
import ai.langstream.api.runner.code.AgentSink;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class CassandraWriterTest {
    @Container
    CassandraContainer cassandra = new CassandraContainer(new DockerImageName("cassandra", "latest"));

    @Test
    void testWrite() throws Exception {
        CqlSession session = CqlSession
                .builder()
                .addContactPoint(this.cassandra.getContactPoint())
                .withLocalDatacenter(this.cassandra.getLocalDatacenter())
                .build();

        session.execute("CREATE KEYSPACE IF NOT EXISTS vsearch WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };");
        session.execute("CREATE TABLE vsearch.products (id int PRIMARY KEY,name TEXT,description TEXT);");

        Map<String, Object> datasourceConfig = Map.of("service", "cassandra",
        "loadBalancing-localDc", cassandra.getLocalDatacenter(),
        "contact-points", cassandra.getContactPoint().getHostString(),
            "port", cassandra.getContactPoint().getPort());


        VectorDBSinkAgent agent = (VectorDBSinkAgent) new AgentCodeRegistry().getAgentCode("vector-db-sink").agentCode();
        Map<String, Object> configuration = new HashMap<>();
        configuration.put("datasource", datasourceConfig);

        configuration.put("table", "vsearch.products");
        configuration.put("mapping", "id=value.id,description=value.description,name=value.name");

        agent.init(configuration);
        agent.start();
        List<Record> committed = new ArrayList<>();
        agent.setCommitCallback(new AgentSink.CommitCallback() {
            @Override
            public void commit(List<Record> records) {
                committed.addAll(records);
            }
        });

        Map<String, Object> value = Map.of("id", "1",
                "description", "test-description", "name", "test-name");
        SimpleRecord record = SimpleRecord.of(null, new ObjectMapper().writeValueAsString(value));
        agent.write(List.of(record));

        assertEquals(committed.get(0), record);
        agent.close();


        ResultSet execute = session.execute("SELECT * FROM vsearch.products WHERE id=1");
        assertEquals(1, execute.all().size());

    }
}
