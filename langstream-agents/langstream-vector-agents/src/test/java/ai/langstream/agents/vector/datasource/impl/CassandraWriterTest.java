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
package ai.langstream.agents.vector.datasource.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.langstream.agents.vector.VectorDBSinkAgent;
import ai.langstream.api.runner.code.AgentCodeRegistry;
import ai.langstream.api.runner.code.AgentContext;
import ai.langstream.api.runner.code.MetricsReporter;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class CassandraWriterTest {
    @Container
    CassandraContainer cassandra =
            new CassandraContainer(new DockerImageName("cassandra", "latest"));

    @Test
    void testWrite() throws Exception {
        CqlSession session =
                CqlSession.builder()
                        .addContactPoint(this.cassandra.getContactPoint())
                        .withLocalDatacenter(this.cassandra.getLocalDatacenter())
                        .build();

        session.execute(
                "CREATE KEYSPACE IF NOT EXISTS vsearch WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };");
        session.execute(
                "CREATE TABLE vsearch.products (id int PRIMARY KEY,name TEXT,description TEXT);");

        Map<String, Object> datasourceConfig =
                Map.of(
                        "service",
                        "cassandra",
                        "loadBalancing-localDc",
                        cassandra.getLocalDatacenter(),
                        "contact-points",
                        cassandra.getContactPoint().getHostString(),
                        "port",
                        cassandra.getContactPoint().getPort());

        VectorDBSinkAgent agent =
                (VectorDBSinkAgent)
                        new AgentCodeRegistry().getAgentCode("vector-db-sink").agentCode();
        Map<String, Object> configuration = new HashMap<>();
        configuration.put("datasource", datasourceConfig);

        configuration.put("table", "vsearch.products");
        configuration.put("mapping", "id=value.id,description=value.description,name=value.name");

        AgentContext agentContext = mock(AgentContext.class);
        when(agentContext.getMetricsReporter()).thenReturn(MetricsReporter.DISABLED);
        agent.init(configuration);
        agent.setContext(agentContext);
        agent.start();
        List<Record> committed = new CopyOnWriteArrayList<>();

        Map<String, Object> value =
                Map.of("id", "1", "description", "test-description", "name", "test-name");
        SimpleRecord record = SimpleRecord.of(null, new ObjectMapper().writeValueAsString(value));
        agent.write(record).thenRun(() -> committed.add(record)).get();

        assertEquals(committed.get(0), record);
        agent.close();

        ResultSet execute = session.execute("SELECT * FROM vsearch.products WHERE id=1");
        assertEquals(1, execute.all().size());
    }

    @Test
    @Disabled
    void testWriteAstra() throws Exception {

        Map<String, Object> datasourceConfig =
                Map.of(
                        "service",
                        "astra",
                        "username",
                        "xxxx",
                        "password",
                        "xxxxx+-ZR9QqyI-z+FfHm7e92eClu,sriPeUoc2OkWALvPoOtvDT0pqtHbDjyzgdKTk5ghMW49tdol8vg-YfjE,qklG-jd_HLK.t",
                        "secureBundle",
                        "base64:xxxx");

        VectorDBSinkAgent agent =
                (VectorDBSinkAgent)
                        new AgentCodeRegistry().getAgentCode("vector-db-sink").agentCode();
        Map<String, Object> configuration = new HashMap<>();
        configuration.put("datasource", datasourceConfig);

        configuration.put("table", "vsearch.products");
        configuration.put("mapping", "id=value.id,description=value.description,name=value.name");

        AgentContext agentContext = mock(AgentContext.class);
        when(agentContext.getMetricsReporter()).thenReturn(MetricsReporter.DISABLED);
        agent.init(configuration);
        agent.setContext(agentContext);
        agent.start();
        List<Record> committed = new CopyOnWriteArrayList<>();

        Map<String, Object> value =
                Map.of("id", "1", "description", "test-description", "name", "test-name");
        SimpleRecord record = SimpleRecord.of(null, new ObjectMapper().writeValueAsString(value));
        agent.write(record).thenRun(() -> committed.add(record)).get();

        assertEquals(committed.get(0), record);
        agent.close();
    }
}
