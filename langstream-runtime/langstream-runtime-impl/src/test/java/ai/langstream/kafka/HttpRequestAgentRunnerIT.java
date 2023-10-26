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
package ai.langstream.kafka;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@Slf4j
@WireMockTest
class HttpRequestAgentRunnerIT extends AbstractKafkaApplicationRunner {

    static WireMockRuntimeInfo wireMockRuntimeInfo;

    @BeforeAll
    static void onBeforeAll(WireMockRuntimeInfo info) {
        wireMockRuntimeInfo = info;
    }

    @Test
    void testGetJson() throws Exception {

        stubFor(
                get("/api/models?name=my-model")
                        .willReturn(
                                okJson(
                                        """
                                        {"id": "my-model",
                                         "created": "2021-08-31T12:00:00Z",
                                         "model": "gpt-35-turbo",
                                         "object": "text-generation",
                                         "choices": [{"text": "It is a car."}]}
                                        """)));

        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                    deletion-mode: delete
                                  - name: "output-topic"
                                    creation-mode: create-if-not-exists
                                    deletion-mode: delete
                                pipeline:
                                  - name: "http-request"
                                    type: "http-request"
                                    input: input-topic
                                    output: output-topic
                                    id: step1
                                    configuration:
                                        output-field: value.api
                                        url: %s/api/models
                                        query-string:
                                            name: "{{{ value.id }}}"

                                """
                                .formatted(wireMockRuntimeInfo.getHttpBaseUrl()));

        final String e1 =
                """
                        {"id":"my-model","classification":"good","api":{"id":"my-model","created":"2021-08-31T12:00:00Z","model":"gpt-35-turbo","object":"text-generation","choices":[{"text":"It is a car."}]}}""";
        runAndAssertMessage(application, e1);
    }

    @Test
    void testGetRawText() throws Exception {

        stubFor(
                get("/api/models?name=my-model")
                        .willReturn(
                                ok("""
                                        some-string""")));

        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                    deletion-mode: delete
                                  - name: "output-topic"
                                    creation-mode: create-if-not-exists
                                    deletion-mode: delete
                                pipeline:
                                  - name: "http-request"
                                    type: "http-request"
                                    input: input-topic
                                    output: output-topic
                                    id: step1
                                    configuration:
                                        output-field: value.api
                                        url: %s/api/models
                                        query-string:
                                            name: "{{{ value.id }}}"

                                """
                                .formatted(wireMockRuntimeInfo.getHttpBaseUrl()));

        final String e1 =
                """
                        {"id":"my-model","classification":"good","api":"some-string"}""";
        runAndAssertMessage(application, e1);
    }

    @Test
    void testPostWithBody() throws Exception {

        stubFor(
                post("/api/models?name=my-model")
                        .withHeader("Content-Type", equalTo("application/json"))
                        .withHeader("Authorization", equalTo("Bearer my-token!"))
                        .withRequestBody(equalTo("{\"id\": \"my-model\"}"))
                        .willReturn(
                                okJson(
                                        """
                                        {"id": "my-model",
                                         "created": "2021-08-31T12:00:00Z",
                                         "model": "gpt-35-turbo",
                                         "object": "text-generation",
                                         "choices": [{"text": "It is a car."}]}
                                        """)));

        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                    deletion-mode: delete
                                  - name: "output-topic"
                                    creation-mode: create-if-not-exists
                                    deletion-mode: delete
                                pipeline:
                                  - name: "http-request"
                                    type: "http-request"
                                    input: input-topic
                                    output: output-topic
                                    id: step1
                                    configuration:
                                        output-field: value.api
                                        url: %s/api/models
                                        query-string:
                                            name: "{{{ value.id }}}"
                                        method: POST
                                        body: '{"id": "{{{ value.id }}}"}'
                                        headers:
                                          Content-Type: application/json
                                          Authorization: Bearer {{{ secrets.s1.token }}}
                                """
                                .formatted(wireMockRuntimeInfo.getHttpBaseUrl()));

        final String e1 =
                """
                        {"id":"my-model","classification":"good","api":{"id":"my-model","created":"2021-08-31T12:00:00Z","model":"gpt-35-turbo","object":"text-generation","choices":[{"text":"It is a car."}]}}""";
        runAndAssertMessage(application, e1);
    }

    private void runAndAssertMessage(Map<String, String> application, String e1) throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};

        // write some data
        try (ApplicationRuntime applicationRuntime =
                deployApplicationWithSecrets(
                        tenant,
                        "app",
                        application,
                        buildInstanceYaml(),
                        """
            secrets:
            - id: s1
              data:
                token: my-token!
            """,
                        expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer();
                    KafkaConsumer<String, String> consumer = createConsumer("output-topic")) {

                sendMessage(
                        "input-topic",
                        "{\"id\":\"my-model\",\"classification\":\"good\"}",
                        producer);
                executeAgentRunners(applicationRuntime);

                waitForMessages(consumer, List.of(e1));
            }
        }
    }
}
