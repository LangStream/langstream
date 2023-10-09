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
package ai.langstream.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import ai.langstream.tests.util.BaseEndToEndTest;
import ai.langstream.tests.util.ConsumeGatewayMessage;
import ai.langstream.tests.util.TestSuites;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Slf4j
@ExtendWith(BaseEndToEndTest.class)
@Tag(TestSuites.CATEGORY_OTHER)
public class S3AzureSourceIT extends BaseEndToEndTest {

    @Test
    public void test() throws Exception {
        final boolean isS3 = codeStorageConfig.type().equals("s3");
        final String tenant = "ten-" + System.currentTimeMillis();
        final String bucketName = "langstream-qa-source-" + tenant;
        final String objectName = "simple-" + tenant + ".pdf";

        installLangStreamCluster(true);

        codeStorageProvider.createBucket(bucketName);
        codeStorageProvider.uploadFromFile(
                Paths.get("src/test/resources/files/simple.pdf").toFile().getAbsolutePath(),
                bucketName,
                objectName);
        setupTenant(tenant);
        final String applicationId = "my-test-app";

        final HashMap<String, String> env = new HashMap<>();
        if (isS3) {
            env.put("S3_ENDPOINT", codeStorageConfig.configuration().get("endpoint"));
            env.put("S3_BUCKET_NAME", bucketName);
            env.put("S3_ACCESS_KEY", codeStorageConfig.configuration().get("access-key"));
            env.put("S3_SECRET_KEY", codeStorageConfig.configuration().get("secret-key"));
        } else {
            env.put("AZURE_ENDPOINT", codeStorageConfig.configuration().get("endpoint"));
            env.put("AZURE_CONTAINER_NAME", bucketName);
            env.put(
                    "AZURE_ACCOUNT_NAME",
                    codeStorageConfig.configuration().get("storage-account-name"));
            env.put(
                    "AZURE_ACCOUNT_KEY",
                    codeStorageConfig.configuration().get("storage-account-key"));
        }

        deployLocalApplicationAndAwaitReady(
                tenant, applicationId, isS3 ? "s3-source" : "azure-blob-source", env, 1);

        // in this timeout is also included the runtime pod startup time
        Awaitility.await()
                .atMost(1, TimeUnit.MINUTES)
                .pollInterval(5, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            assertFalse(codeStorageProvider.objectExists(bucketName, objectName));
                        });

        final ConsumeGatewayMessage message =
                consumeOneMessageFromGateway(
                        applicationId, "consume-chunks", "--position", "earliest");
        log.info("output {}", message);
        final Map<String, Object> asMap = message.recordValueAsMap();
        assertEquals("en", asMap.get("language"));
        assertEquals("6", asMap.get("chunk_num_tokens"));
        assertEquals(objectName, asMap.get("name"));
    }
}
