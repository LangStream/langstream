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
package ai.langstream.agents.s3;

import static org.junit.Assert.assertSame;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import ai.langstream.api.runner.code.AgentCodeRegistry;
import ai.langstream.api.runner.code.AgentContext;
import ai.langstream.api.runner.code.AgentProcessor;
import ai.langstream.api.runner.code.AgentSource;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.MetricsReporter;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectArgs;
import io.minio.Result;
import io.minio.messages.Item;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class S3SourceTest {

    private static final AgentCodeRegistry AGENT_CODE_REGISTRY = new AgentCodeRegistry();
    private static final DockerImageName localstackImage =
            DockerImageName.parse("localstack/localstack:2.2.0");

    @Container
    private static final LocalStackContainer localstack =
            new LocalStackContainer(localstackImage).withServices(S3);

    private static MinioClient minioClient;

    @BeforeAll
    static void setup() {
        minioClient =
                MinioClient.builder()
                        .endpoint(localstack.getEndpointOverride(S3).toString())
                        .build();
    }

    @Test
    void testProcess() throws Exception {
        // Add some objects to the bucket
        String bucket = "langstream-test-" + UUID.randomUUID();
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucket).build());
        AgentProcessor agentProcessor = buildAgentProcessor(bucket);
        String content = "test-content-";
        for (int i = 0; i < 2; i++) {
            String s = content + i;
            minioClient.putObject(
                    PutObjectArgs.builder().bucket(bucket).object("test'-" + i + ".txt").stream(
                                    new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)),
                                    s.length(),
                                    -1)
                            .build());
        }
        // List objects from the bucket
        Iterable<Result<Item>> results =
                minioClient.listObjects(ListObjectsArgs.builder().bucket(bucket).build());

        for (Result<Item> result : results) {
            Item item = result.get();
            // Display the object name in the logs
            System.out.println(item.objectName());
        }

        // Create a input record that specifies the first file
        String objectName = "test'-0.txt";
        SimpleRecord someRecord =
                SimpleRecord.builder()
                        .value("{\"objectName\": \"" + objectName + "\"}")
                        .headers(
                                List.of(
                                        new SimpleRecord.SimpleHeader(
                                                "original", "Some session id")))
                        .build();

        // Process the record
        List<AgentProcessor.SourceRecordAndResult> resultsForRecord = new ArrayList<>();
        agentProcessor.process(List.of(someRecord), resultsForRecord::add);

        // Should be a record for the file
        assertEquals(1, resultsForRecord.size());

        // the processor must pass downstream the original record
        Record emittedToDownstream = resultsForRecord.get(0).sourceRecord();
        assertSame(emittedToDownstream, someRecord);

        // The resulting record should have the file content as the value
        assertArrayEquals(
                "test-content-0".getBytes(StandardCharsets.UTF_8),
                (byte[]) resultsForRecord.get(0).resultRecords().get(0).value());
        // The resulting record should have the file name as the key
        assertEquals(objectName, resultsForRecord.get(0).resultRecords().get(0).key());

        // Check headers
        Collection<Header> headers = resultsForRecord.get(0).resultRecords().get(0).headers();

        // Make sure the name header matches the object name
        Optional<Header> foundNameHeader =
                headers.stream()
                        .filter(
                                header ->
                                        "name".equals(header.key())
                                                && objectName.equals(header.value()))
                        .findFirst();

        assertTrue(
                foundNameHeader.isPresent()); // Check that the object name is passed in the record

        // Make sure the original header matches the passed in header
        Optional<Header> foundOrigHeader =
                headers.stream()
                        .filter(
                                header ->
                                        "original".equals(header.key())
                                                && "Some session id".equals(header.value()))
                        .findFirst();

        assertTrue(
                foundOrigHeader.isPresent()); // Check that the object name is passed in the record

        // Get the next file
        String secondObjectName = "test'-1.txt";
        someRecord =
                SimpleRecord.builder()
                        .value("{\"objectName\": \"" + secondObjectName + "\"}")
                        .headers(
                                List.of(
                                        new SimpleRecord.SimpleHeader(
                                                "original", "Some session id")))
                        .build();

        resultsForRecord = new ArrayList<>();
        agentProcessor.process(List.of(someRecord), resultsForRecord::add);

        // Make sure the second file is processed
        assertEquals(1, resultsForRecord.size()); // assertEquals(
    }

    @Test
    void testProcessFromDirectory() throws Exception {
        // Add some objects to the bucket in a directory
        String bucket = "langstream-test-" + UUID.randomUUID();
        String directory = "test-dir/";
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucket).build());
        AgentProcessor agentProcessor = buildAgentProcessor(bucket);
        String content = "test-content-";
        for (int i = 0; i < 2; i++) {
            String s = content + i;
            minioClient.putObject(
                    PutObjectArgs.builder()
                            .bucket(bucket)
                            .object(directory + "test-" + i + ".txt")
                            .stream(
                                    new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)),
                                    s.length(),
                                    -1)
                            .build());
        }

        // Process the first file in the directory
        String firstObjectName = directory + "test-0.txt";
        SimpleRecord firstRecord =
                SimpleRecord.builder()
                        .value("{\"objectName\": \"" + firstObjectName + "\"}")
                        .headers(
                                List.of(
                                        new SimpleRecord.SimpleHeader(
                                                "original", "Some session id")))
                        .build();

        List<AgentProcessor.SourceRecordAndResult> resultsForFirstRecord = new ArrayList<>();
        agentProcessor.process(List.of(firstRecord), resultsForFirstRecord::add);
        // Make sure the first file is processed and that the original record is passed downstream
        assertEquals(1, resultsForFirstRecord.size());
        assertSame(firstRecord, resultsForFirstRecord.get(0).sourceRecord());
        // Check that the content of the first record is the content of the first file
        assertArrayEquals(
                "test-content-0".getBytes(StandardCharsets.UTF_8),
                (byte[]) resultsForFirstRecord.get(0).resultRecords().get(0).value());
        // Check that the key of the first record is the name of the first file
        assertEquals(firstObjectName, resultsForFirstRecord.get(0).resultRecords().get(0).key());

        // Check headers for first record
        // The name header contains the file name
        Collection<Header> firstRecordHeaders =
                resultsForFirstRecord.get(0).resultRecords().get(0).headers();
        assertTrue(
                firstRecordHeaders.stream()
                        .anyMatch(
                                header ->
                                        "name".equals(header.key())
                                                && firstObjectName.equals(header.value())));
        // The original header contains the original header from the record
        assertTrue(
                firstRecordHeaders.stream()
                        .anyMatch(
                                header ->
                                        "original".equals(header.key())
                                                && "Some session id".equals(header.value())));

        // Process the second file in the directory
        String secondObjectName = directory + "test-1.txt";
        SimpleRecord secondRecord =
                SimpleRecord.builder()
                        .value("{\"objectName\": \"" + secondObjectName + "\"}")
                        .headers(
                                List.of(
                                        new SimpleRecord.SimpleHeader(
                                                "original", "Some session id")))
                        .build();

        List<AgentProcessor.SourceRecordAndResult> resultsForSecondRecord = new ArrayList<>();
        agentProcessor.process(List.of(secondRecord), resultsForSecondRecord::add);

        assertEquals(1, resultsForSecondRecord.size());
        assertSame(secondRecord, resultsForSecondRecord.get(0).sourceRecord());

        assertArrayEquals(
                "test-content-1".getBytes(StandardCharsets.UTF_8),
                (byte[]) resultsForSecondRecord.get(0).resultRecords().get(0).value());

        // Check headers for second record
        Collection<Header> secondRecordHeaders =
                resultsForSecondRecord.get(0).resultRecords().get(0).headers();
        assertTrue(
                secondRecordHeaders.stream()
                        .anyMatch(
                                header ->
                                        "name".equals(header.key())
                                                && secondObjectName.equals(header.value())));
        assertTrue(
                secondRecordHeaders.stream()
                        .anyMatch(
                                header ->
                                        "original".equals(header.key())
                                                && "Some session id".equals(header.value())));
    }

    @Test
    void testRead() throws Exception {
        String bucket = "langstream-test-" + UUID.randomUUID();
        AgentSource agentSource = buildAgentSource(bucket);
        String content = "test-content-";
        for (int i = 0; i < 10; i++) {
            String s = content + i;
            minioClient.putObject(
                    PutObjectArgs.builder().bucket(bucket).object("test-" + i + ".txt").stream(
                                    new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)),
                                    s.length(),
                                    -1)
                            .build());
        }

        List<Record> read = agentSource.read();
        assertEquals(1, read.size());
        assertArrayEquals(
                "test-content-0".getBytes(StandardCharsets.UTF_8), (byte[]) read.get(0).value());

        // DO NOT COMMIT, the source should not return the same objects

        List<Record> read2 = agentSource.read();

        assertEquals(1, read2.size());
        assertArrayEquals(
                "test-content-1".getBytes(StandardCharsets.UTF_8), (byte[]) read2.get(0).value());

        // COMMIT (out of order)
        agentSource.commit(read2);
        agentSource.commit(read);

        Iterator<Result<Item>> results =
                minioClient
                        .listObjects(ListObjectsArgs.builder().bucket(bucket).build())
                        .iterator();
        for (int i = 2; i < 10; i++) {
            Result<Item> item = results.next();
            assertEquals("test-" + i + ".txt", item.get().objectName());
        }

        List<Record> all = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            all.addAll(agentSource.read());
        }

        agentSource.commit(all);
        all.clear();

        results =
                minioClient
                        .listObjects(ListObjectsArgs.builder().bucket(bucket).build())
                        .iterator();
        assertFalse(results.hasNext());

        for (int i = 0; i < 10; i++) {
            all.addAll(agentSource.read());
        }
        agentSource.commit(all);
        agentSource.commit(List.of());
    }

    @Test
    void emptyBucket() throws Exception {
        String bucket = "langstream-test-" + UUID.randomUUID();
        AgentSource agentSource = buildAgentSource(bucket);
        assertFalse(
                minioClient
                        .listObjects(ListObjectsArgs.builder().bucket(bucket).build())
                        .iterator()
                        .hasNext());
        agentSource.commit(List.of());
        List<Record> read = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            read.addAll(agentSource.read());
        }
        assertFalse(
                minioClient
                        .listObjects(ListObjectsArgs.builder().bucket(bucket).build())
                        .iterator()
                        .hasNext());
        agentSource.commit(read);
    }

    @Test
    void commitNonExistent() throws Exception {
        String bucket = "langstream-test-" + UUID.randomUUID();
        AgentSource agentSource = buildAgentSource(bucket);
        String content = "test-content";
        minioClient.putObject(
                PutObjectArgs.builder().bucket(bucket).object("test").stream(
                                new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)),
                                content.length(),
                                -1)
                        .build());
        List<Record> read = agentSource.read();
        minioClient.removeObject(RemoveObjectArgs.builder().bucket(bucket).object("test").build());
        agentSource.commit(read);
    }

    private AgentSource buildAgentSource(String bucket) throws Exception {
        AgentSource agentSource =
                (AgentSource) AGENT_CODE_REGISTRY.getAgentCode("s3-source").agentCode();
        Map<String, Object> configs = new HashMap<>();
        String endpoint = localstack.getEndpointOverride(S3).toString();
        configs.put("endpoint", endpoint);
        configs.put("bucketName", bucket);
        agentSource.init(configs);
        AgentContext context = mock(AgentContext.class);
        when(context.getMetricsReporter()).thenReturn(MetricsReporter.DISABLED);
        agentSource.setContext(context);
        agentSource.start();
        return agentSource;
    }

    private AgentProcessor buildAgentProcessor(String bucket) throws Exception {
        AgentProcessor agent =
                (AgentProcessor) AGENT_CODE_REGISTRY.getAgentCode("s3-processor").agentCode();
        Map<String, Object> configs = new HashMap<>();
        String endpoint = localstack.getEndpointOverride(S3).toString();
        configs.put("endpoint", endpoint);
        configs.put("bucketName", bucket);
        configs.put("objectName", "{{{ value.objectName }}}");
        agent.init(configs);
        AgentContext context = mock(AgentContext.class);
        when(context.getMetricsReporter()).thenReturn(MetricsReporter.DISABLED);
        agent.setContext(context);
        agent.start();
        return agent;
    }

    @Test
    void testIsExtensionAllowed() {
        assertTrue(S3Source.isExtensionAllowed("aaa", Set.of("*")));
        assertTrue(S3Source.isExtensionAllowed("", Set.of("*")));
        assertTrue(S3Source.isExtensionAllowed(".aaa", Set.of("*")));
        assertTrue(S3Source.isExtensionAllowed("aaa.", Set.of("*")));

        assertFalse(S3Source.isExtensionAllowed("aaa", Set.of("aaa")));
        assertFalse(S3Source.isExtensionAllowed("", Set.of("aaa")));
        assertTrue(S3Source.isExtensionAllowed(".aaa", Set.of("aaa")));
        assertFalse(S3Source.isExtensionAllowed("aaa.", Set.of("aaa")));

        assertFalse(S3Source.isExtensionAllowed("aaa", Set.of("bbb")));
        assertFalse(S3Source.isExtensionAllowed("", Set.of("bbb")));
        assertFalse(S3Source.isExtensionAllowed(".aaa", Set.of("bbb")));
        assertFalse(S3Source.isExtensionAllowed("aaa.", Set.of("b")));
    }
}
