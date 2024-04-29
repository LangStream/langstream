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

import com.couchbase.client.java.Cluster;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.couchbase.CouchbaseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
public class CouchbaseWriterTest {

    BucketDefinition bucketDefinition = new BucketDefinition("bucket-name");

    @Container
    private CouchbaseContainer container =
            new CouchbaseContainer("couchbase/server").withBucket(bucketDefinition);

    @Test
    void testWrite() throws Exception {
        Cluster cluster =
                Cluster.connect(
                        container.getConnectionString(),
                        container.getUsername(),
                        container.getPassword());

        //         List<Float> vector = new ArrayList<>();
        //         List<Float> vector2 = new ArrayList<>();
        //         for (int i = 0; i < DIMENSIONS; i++) {
        //             vector.add(i * 1f / DIMENSIONS);
        //             vector2.add((i + 1) * 1f / DIMENSIONS);
        //         }
        //         String vectorAsString = vector.toString();
        //         String vector2AsString = vector2.toString();

        //         SimpleRecord record =
        //                 SimpleRecord.of(
        //                         "{\"name\": \"doc1\", \"chunk_id\": 1}",
        //                         """
        //                                 {
        //                                     "vector": %s,
        //                                     "text": "Lorem ipsum..."
        //                                 }
        //                                 """
        //                                 .formatted(vectorAsString));
        //         collection.upsert(
        //                 "doc1", record,
        // UpsertOptions.upsertOptions().expiry(Duration.ofHours(1)));

        //         GetResult getResult = collection.get("doc1");
        //         assertEquals("doc1", getResult.contentAsObject().getString("name"));
        //         assertEquals("Lorem ipsum...", getResult.contentAsObject().getString("text"));

        //         SimpleRecord recordUpdated =
        //                 SimpleRecord.of(
        //                         "{\"name\": \"doc1\", \"chunk_id\": 1}",
        //                         """
        //                                 {
        //                                     "vector": %s,
        //                                     "text": "Lorem ipsum changed..."
        //                                 }
        //                                 """
        //                                 .formatted(vector2AsString));
        //         collection.upsert(
        //                 "doc1", recordUpdated,
        // UpsertOptions.upsertOptions().expiry(Duration.ofHours(1)));
        //         GetResult getResultUpdated = collection.get("doc1");
        //         assertEquals("doc1", getResultUpdated.contentAsObject().getString("name"));
        //         assertEquals(
        //                 "Lorem ipsum changed...",
        // getResultUpdated.contentAsObject().getString("text"));

        //         collection.remove("doc1");

        //         assertFalse(collection.exists("doc1").exists());
    }
}
