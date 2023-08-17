/**
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
package ai.langstream.agents.webcrawler;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;
import ai.langstream.api.runner.code.AgentCodeRegistry;
import ai.langstream.api.runner.code.AgentSource;
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
import java.util.UUID;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
@Slf4j
public class WebCrawlerSourceTest {

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
    @Disabled
    void testRead() throws Exception {
        String bucket = "langstream-test-" + UUID.randomUUID();
        String url = "https://docs.langstream.ai/";
        String allowed = "https://docs.langstream.ai/";
        AgentSource agentSource = buildAgentSource(bucket, allowed, url);
        List<Record> read = agentSource.read();
        while (!read.isEmpty()) {
            log.info("read: {}", read);
            read = agentSource.read();
        }
    }


    private AgentSource buildAgentSource(String bucket, String domain, String seedUrl) throws Exception {
        AgentSource agentSource = (AgentSource) AGENT_CODE_REGISTRY.getAgentCode("webcrawler-source");
        Map<String, Object> configs = new HashMap<>();
        String endpoint = localstack.getEndpointOverride(S3).toString();
        configs.put("endpoint", endpoint);
        configs.put("bucketName", bucket);
        configs.put("seed-urls", seedUrl);
        configs.put("allowed-domains", domain);
        agentSource.init(configs);
        agentSource.start();
        return agentSource;
    }
}
