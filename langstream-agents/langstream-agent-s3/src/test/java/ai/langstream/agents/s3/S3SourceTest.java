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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import ai.langstream.api.runner.code.AgentCodeRegistry;
import ai.langstream.api.runner.code.AgentContext;
import ai.langstream.api.runner.code.AgentSource;
import ai.langstream.api.runner.code.MetricsReporter;
import ai.langstream.api.runner.code.Record;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectArgs;
import io.minio.Result;
import io.minio.messages.Item;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
