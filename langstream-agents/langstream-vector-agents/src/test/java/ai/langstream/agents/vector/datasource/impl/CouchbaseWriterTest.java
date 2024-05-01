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

import ai.langstream.agents.vector.couchbase.CouchbaseWriter;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.Record;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
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
    public void testUpsertAndRetrieve() throws ExecutionException, InterruptedException {
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

        // Retrieve the record from the Couchbase collection
        Collection collection =
                Cluster.connect(
                                container.getConnectionString(),
                                container.getUsername(),
                                container.getPassword())
                        .bucket("bucket-name")
                        .defaultCollection();
        try {
            writer.close();
        } catch (Exception e) {
            // Handle the exception
            e.printStackTrace();
        }
    }
}
