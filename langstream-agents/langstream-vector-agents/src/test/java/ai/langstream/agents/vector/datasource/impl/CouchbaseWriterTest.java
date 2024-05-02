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

import ai.langstream.agents.vector.couchbase.CouchbaseWriter;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.Record;
// Add missing import statement
import com.couchbase.client.java.kv.GetResult;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.couchbase.CouchbaseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Slf4j
@Testcontainers
public class CouchbaseWriterTest {

    BucketDefinition bucketDefinition = new BucketDefinition("bucket-name");

    // Explicitly declare the image as a compatible substitute
    private DockerImageName couchbaseImage =
            DockerImageName.parse("couchbase/server:7.6.1")
                    .asCompatibleSubstituteFor("couchbase/server");

    // Initialize the Couchbase container
    @Container
    private CouchbaseContainer container =
            new CouchbaseContainer(couchbaseImage)
                    .withBucket(bucketDefinition)
                    .waitingFor(Wait.forHttp("/pools").withStartupTimeout(Duration.ofMinutes(10)))
                    .withExposedPorts(11210);

    @Test
    public void testUpsertAndRetrieve() throws Exception {
        // Set up CouchbaseWriter
        Map<String, Object> dataSourceConfig = new HashMap<>();
        dataSourceConfig.put("connectionString", container.getConnectionString());
        dataSourceConfig.put("username", container.getUsername());
        dataSourceConfig.put("password", container.getPassword());
        dataSourceConfig.put("bucketName", "bucket-name");

        CouchbaseWriter.CouchbaseDatabaseWriter writer =
                new CouchbaseWriter().createImplementation(dataSourceConfig);

        // Create a sample record

        String docId = "test-doc";
        Map<String, Object> content = new HashMap<>();
        content.put("field1", "value1");
        content.put("field2", 123);
        // add a vector field
        content.put("vector", new double[] {1.0, 2.0, 3.0});
        Record record =
                new Record() {
                    @Override
                    public Object key() {
                        return docId;
                    }

                    @Override
                    public Object value() {
                        return content;
                    }

                    @Override
                    public String origin() {
                        // TODO Auto-generated method stub
                        throw new UnsupportedOperationException("Unimplemented method 'origin'");
                    }

                    @Override
                    public Long timestamp() {
                        // TODO Auto-generated method stub
                        throw new UnsupportedOperationException("Unimplemented method 'timestamp'");
                    }

                    @Override
                    public java.util.Collection<Header> headers() {
                        // TODO Auto-generated method stub
                        throw new UnsupportedOperationException("Unimplemented method 'headers'");
                    }
                };

        // Upsert the record
        writer.upsert(record, null).get();

        // Retrieve and verify the document
        GetResult result = writer.collection.get(docId);
        System.out.println(result.contentAsObject());
        assertEquals("value1", result.contentAsObject().get("field1").toString());
        assertEquals(123, (Integer) result.contentAsObject().get("field2"));

        // add a pause to allow the test to complete
        // Thread.sleep(10000000);

        // Ensure the cluster connection is closed after the test
        writer.close();
    }
}
