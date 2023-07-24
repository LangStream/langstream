package com.datastax.oss.sga.agents.s3;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;
import com.datastax.oss.sga.api.runner.code.AgentCodeRegistry;
import com.datastax.oss.sga.api.runner.code.AgentSource;
import com.datastax.oss.sga.api.runner.code.Record;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectArgs;
import io.minio.Result;
import io.minio.messages.Item;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
    private static final DockerImageName localstackImage = DockerImageName.parse("localstack/localstack:2.2.0");

    @Container
    private static final LocalStackContainer localstack = new LocalStackContainer(localstackImage)
        .withServices(S3);

    private static MinioClient minioClient;

    @BeforeAll
    static void setup() {
        minioClient = MinioClient.builder()
            .endpoint(localstack.getEndpointOverride(S3).toString())
            .build();
    }

    @Test
    void testRead() throws Exception {
        String bucket = "sga-test-" + UUID.randomUUID();
        AgentSource agentSource = buildAgentSource(bucket);
        String content = "test-content-";
        for (int i = 0; i < 10; i++) {
            String s = content + i;
            minioClient.putObject(
                PutObjectArgs.builder()
                    .bucket(bucket)
                    .object("test-" + i)
                    .stream(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)), s.length(), -1)
                    .build()
            );
        }

        List<Record> read = agentSource.read();
        assertEquals(1, read.size());
        assertArrayEquals("test-content-0".getBytes(StandardCharsets.UTF_8), (byte[]) read.get(0).value());

        read = agentSource.read();
        assertEquals(1, read.size());
        assertArrayEquals("test-content-1".getBytes(StandardCharsets.UTF_8), (byte[]) read.get(0).value());

        agentSource.commit();

        Iterator<Result<Item>> results = minioClient.listObjects(ListObjectsArgs.builder().bucket(bucket).build()).iterator();
        for (int i = 2; i < 10; i++) {
            Result<Item> item = results.next();
            assertEquals("test-" + i, item.get().objectName());
        }

        for (int i = 0; i < 8; i++) {
            agentSource.read();
        }

        agentSource.commit();
        results = minioClient.listObjects(ListObjectsArgs.builder().bucket(bucket).build()).iterator();
        assertFalse(results.hasNext());

        for (int i = 0; i < 10; i++) {
            agentSource.read();
        }
        agentSource.commit();
        agentSource.commit();
    }

    @Test
    void emptyBucket() throws Exception {
        String bucket = "sga-test-" + UUID.randomUUID();
        AgentSource agentSource = buildAgentSource(bucket);
        assertFalse(minioClient.listObjects(ListObjectsArgs.builder().bucket(bucket).build()).iterator().hasNext());
        agentSource.commit();
        for (int i = 0; i < 10; i++) {
            agentSource.read();
        }
        assertFalse(minioClient.listObjects(ListObjectsArgs.builder().bucket(bucket).build()).iterator().hasNext());
        agentSource.commit();
    }

    @Test
    void commitNonExistent() throws Exception {
        String bucket = "sga-test-" + UUID.randomUUID();
        AgentSource agentSource = buildAgentSource(bucket);
        String content = "test-content";
        minioClient.putObject(
            PutObjectArgs.builder()
                .bucket(bucket)
                .object("test")
                .stream(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)), content.length(), -1)
                .build());
        agentSource.read();
        minioClient.removeObject(RemoveObjectArgs.builder().bucket(bucket).object("test").build());
        agentSource.commit();
    }

    private AgentSource buildAgentSource(String bucket) throws Exception {
        AgentSource agentSource = (AgentSource) AGENT_CODE_REGISTRY.getAgentCode("s3-source");
        Map<String, Object> configs = new HashMap<>();
        String endpoint = localstack.getEndpointOverride(S3).toString();
        configs.put("endpoint", endpoint);
        configs.put("bucketName", bucket);
        agentSource.init(configs);
        agentSource.start();
        return agentSource;
    }
}
