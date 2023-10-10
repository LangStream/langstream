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

import static ai.langstream.tests.TextCompletionsIT.getAppEnvForAIServiceProvider;

import ai.langstream.tests.util.BaseEndToEndTest;
import ai.langstream.tests.util.ConsumeGatewayMessage;
import ai.langstream.tests.util.TestSuites;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Slf4j
@ExtendWith(BaseEndToEndTest.class)
@Tag(TestSuites.CATEGORY_NEEDS_CREDENTIALS)
public class WebCrawlerToVectorIT extends BaseEndToEndTest {

    static Map<String, String> appEnv;

    @BeforeAll
    public static void checkCredentials() {
        appEnv = getAppEnvForAIServiceProvider();
        appEnv.putAll(
                getAppEnvMapFromSystem(
                        List.of("CHAT_COMPLETIONS_MODEL", "CHAT_COMPLETIONS_SERVICE")));
        appEnv.putAll(getAppEnvMapFromSystem(List.of("EMBEDDINGS_MODEL", "EMBEDDINGS_SERVICE")));

        appEnv.putAll(
                getAppEnvMapFromSystem(
                        List.of(
                                "ASTRA_TOKEN",
                                "ASTRA_CLIENT_ID",
                                "ASTRA_SECRET",
                                "ASTRA_ENVIRONMENT",
                                "ASTRA_DATABASE")));
    }

    @Test
    public void test() throws Exception {
        if (!codeStorageConfig.type().equals("s3")) {
            throw new IllegalStateException(
                    "This test can only run with S3 code storage, but got: "
                            + codeStorageConfig.type());
        }

        appEnv.put("S3_ENDPOINT", codeStorageConfig.configuration().get("endpoint"));
        appEnv.put("S3_ACCESS_KEY", codeStorageConfig.configuration().get("access-key"));
        appEnv.put("S3_SECRET_KEY", codeStorageConfig.configuration().get("secret-key"));
        installLangStreamCluster(true);
        final String tenant = "ten-" + System.currentTimeMillis();
        setupTenant(tenant);

        final String applicationId = "app";

        deployLocalApplicationAndAwaitReady(
                tenant, applicationId, "webcrawler-to-vector", appEnv, 3);

        final String sessionId = UUID.randomUUID().toString();

        boolean ok = false;
        for (int i = 0; i < 10; i++) {
            executeCommandOnClient(
                    "bin/langstream gateway produce %s user-input -v 'When was released LangStream 0.0.20? Write it in format yyyy-dd-mm.' -p sessionId=%s"
                            .formatted(applicationId, sessionId)
                            .split(" "));
            final ConsumeGatewayMessage message =
                    consumeOneMessageFromGateway(
                            applicationId,
                            "llm-debug",
                            "-p sessionId=%s --connect-timeout 30".formatted(sessionId).split(" "));
            if (message == null) {
                Thread.sleep(5000);
                continue;
            }

            log.info("Output: {}", message);
            final String asString = (String) message.getRecord().getValue();
            final String answer = (String) JSON_MAPPER.readValue(asString, Map.class).get("answer");
            log.info("Answer: {}", answer);
            if (answer.contains("2023-09-19")) {
                ok = true;
                break;
            }
            Thread.sleep(5000);
        }
        if (!ok) {
            Assertions.fail(
                    "the chatbot did not answer correctly, maybe the crawler didn't finished yet?");
        }
    }
}
