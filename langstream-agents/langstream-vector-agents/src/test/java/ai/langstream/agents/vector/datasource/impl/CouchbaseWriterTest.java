package ai.langstream.agents.vector.datasource.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import ai.langstream.api.runner.code.SimpleRecord;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.UpsertOptions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
public class CouchbaseWriterTest {

    static final int DIMENSIONS = 1356;

    @Container
    private GenericContainer couchbaseContainer =
            new GenericContainer("couchbase/server")
                    .withExposedPorts(8091, 8092, 8093, 8094, 11210)
                    .withEnv("COUCHBASE_SERVER_CONFIG_USER", "Administrator")
                    .withEnv("COUCHBASE_SERVER_CONFIG_PASSWORD", "password");

    @Test
    void testWrite() throws Exception {
        Cluster cluster = Cluster.connect("localhost", "Administrator", "password");
        Collection collection = cluster.bucket("bucket-name").defaultCollection();

        List<Float> vector = new ArrayList<>();
        List<Float> vector2 = new ArrayList<>();
        for (int i = 0; i < DIMENSIONS; i++) {
            vector.add(i * 1f / DIMENSIONS);
            vector2.add((i + 1) * 1f / DIMENSIONS);
        }
        String vectorAsString = vector.toString();
        String vector2AsString = vector2.toString();

        SimpleRecord record =
                SimpleRecord.of(
                        "{\"name\": \"doc1\", \"chunk_id\": 1}",
                        """
                                {
                                    "vector": %s,
                                    "text": "Lorem ipsum..."
                                }
                                """
                                .formatted(vectorAsString));
        collection.upsert(
                "doc1", record, UpsertOptions.upsertOptions().expiry(Duration.ofHours(1)));

        GetResult getResult = collection.get("doc1");
        assertEquals("doc1", getResult.contentAsObject().getString("name"));
        assertEquals("Lorem ipsum...", getResult.contentAsObject().getString("text"));

        SimpleRecord recordUpdated =
                SimpleRecord.of(
                        "{\"name\": \"doc1\", \"chunk_id\": 1}",
                        """
                                {
                                    "vector": %s,
                                    "text": "Lorem ipsum changed..."
                                }
                                """
                                .formatted(vector2AsString));
        collection.upsert(
                "doc1", recordUpdated, UpsertOptions.upsertOptions().expiry(Duration.ofHours(1)));
        GetResult getResultUpdated = collection.get("doc1");
        assertEquals("doc1", getResultUpdated.contentAsObject().getString("name"));
        assertEquals(
                "Lorem ipsum changed...", getResultUpdated.contentAsObject().getString("text"));

        collection.remove("doc1");

        assertFalse(collection.exists("doc1").exists());
    }
}
