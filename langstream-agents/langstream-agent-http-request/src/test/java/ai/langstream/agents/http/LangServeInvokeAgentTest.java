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
package ai.langstream.agents.http;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.junit.jupiter.api.Assertions.*;

import ai.langstream.api.runner.code.AgentContext;
import ai.langstream.api.runner.code.AgentProcessor;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.RecordSink;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.runner.topics.TopicAdmin;
import ai.langstream.api.runner.topics.TopicConnectionProvider;
import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runner.topics.TopicProducer;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

@WireMockTest
@Slf4j
class LangServeInvokeAgentTest {

    @Test
    void testInvokeJSONMap(WireMockRuntimeInfo wireMockRuntimeInfo) throws Exception {
        String response =
                """
                        {"output":{"content":"Why don't cats play poker in the wild? Too many cheetahs!","additional_kwargs":{},"type":"ai","example":false},"callback_events":[]}
                        """;
        testInvoke(wireMockRuntimeInfo, response);
    }

    @Test
    void testInvokeJSONMapWithoutContent(WireMockRuntimeInfo wireMockRuntimeInfo) throws Exception {
        String response =
                """
                        {"output":"Why don't cats play poker in the wild? Too many cheetahs!"}
                        """;
        testInvoke(wireMockRuntimeInfo, response);
    }

    @Test
    void testInvokeJSONString(WireMockRuntimeInfo wireMockRuntimeInfo) throws Exception {
        String response =
                """
                        "Why don't cats play poker in the wild? Too many cheetahs!"
                        """;
        testInvoke(wireMockRuntimeInfo, response);
    }

    private void testInvoke(WireMockRuntimeInfo wireMockRuntimeInfo, String response)
            throws Exception {
        stubFor(
                post("/chain/invoke")
                        .withRequestBody(
                                equalTo("""
                        {"input":{"topic":"cats"}}"""))
                        .willReturn(okJson(response)));
        Map<String, Object> configuration =
                Map.of(
                        "fields",
                        List.of(Map.of("name", "topic", "expression", "value.foo")),
                        "url",
                        wireMockRuntimeInfo.getHttpBaseUrl() + "/chain/invoke",
                        "output-field",
                        "value",
                        "debug",
                        true);
        try (LangServeInvokeAgent agent = new LangServeInvokeAgent(); ) {
            agent.init(configuration);
            agent.start();
            List<AgentProcessor.SourceRecordAndResult> records = new CopyOnWriteArrayList<>();
            RecordSink sink = (records::add);

            SimpleRecord input =
                    SimpleRecord.of(
                            null,
                            """
                    {
                       "foo": "cats"
                    }
                    """);
            agent.processRecord(input, sink);

            Awaitility.await()
                    .untilAsserted(
                            () -> {
                                assertEquals(1, records.size());
                            });

            Record record = records.get(0).resultRecords().get(0);
            assertEquals(
                    """
                    Why don't cats play poker in the wild? Too many cheetahs!""",
                    record.value());
        }
    }

    @Test
    void testStreamingJSONMap(WireMockRuntimeInfo wireMockRuntimeInfo) throws Exception {
        String response =
                """
                event: data
                data: {"content": "", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": "Why", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": " don", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": "'t", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": " cats", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": " play", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": " poker", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": " in", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": " the", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": " wild", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": "?\\n\\n", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": "Too", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": " many", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": " che", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": "et", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": "ah", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": "s", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": "!", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": "", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: end""";
        testStreamingOutput(wireMockRuntimeInfo, response);
    }

    @Test
    void testStreamingJSONString(WireMockRuntimeInfo wireMockRuntimeInfo) throws Exception {
        String response =
                """
                event: data
                data: ""

                event: data
                data: "Why"

                event: data
                data: " don"

                event: data
                data: "'t"

                event: data
                data: " cats"

                event: data
                data: " play"

                event: data
                data: " poker"

                event: data
                data: " in"

                event: data
                data: " the"

                event: data
                data: " wild"

                event: data
                data: "?\\n\\n"

                event: data
                data: "Too"

                event: data
                data: " many"

                event: data
                data: " che"

                event: data
                data: "et"

                event: data
                data: "ah"

                event: data
                data: "s"

                event: data
                data: "!"

                event: data
                data: ""

                event: end""";
        testStreamingOutput(wireMockRuntimeInfo, response);
    }

    private void testStreamingOutput(WireMockRuntimeInfo wireMockRuntimeInfo, String response)
            throws Exception {
        stubFor(
                post("/chain/stream")
                        .withRequestBody(
                                equalTo("""
                        {"input":{"topic":"cats"}}"""))
                        .willReturn(ok(response)));

        Map<String, Object> configuration =
                Map.of(
                        "fields",
                        List.of(Map.of("name", "topic", "expression", "value.foo")),
                        "url",
                        wireMockRuntimeInfo.getHttpBaseUrl() + "/chain/stream",
                        "output-field",
                        "value",
                        "debug",
                        true,
                        "stream-to-topic",
                        "some-topic");

        List<Record> streamingAnswers = new ArrayList<>();

        try (LangServeInvokeAgent agent = new LangServeInvokeAgent(); ) {
            agent.init(configuration);

            setupMockTopicProducer(streamingAnswers, agent);
            agent.start();
            List<AgentProcessor.SourceRecordAndResult> records = new CopyOnWriteArrayList<>();
            RecordSink sink = (records::add);

            SimpleRecord input =
                    SimpleRecord.of(
                            null,
                            """
                    {
                       "foo": "cats"
                    }
                    """);
            agent.processRecord(input, sink);

            Awaitility.await()
                    .atMost(1, TimeUnit.DAYS)
                    .untilAsserted(
                            () -> {
                                assertEquals(1, records.size());
                            });

            streamingAnswers.forEach(
                    record -> {
                        log.info("Answer {}", record);
                    });

            Record record = records.get(0).resultRecords().get(0);
            log.info("Main answer: {}", record);
            assertEquals(
                    """
                    Why don't cats play poker in the wild?

                    Too many cheetahs!""",
                    record.value());

            assertEquals("Why", streamingAnswers.get(0).value());
            assertEquals(" don't", streamingAnswers.get(1).value());
            assertEquals(" cats play poker in", streamingAnswers.get(2).value());
            assertEquals(" the wild?\n\nToo many cheetah", streamingAnswers.get(3).value());
            assertEquals("s!", streamingAnswers.get(4).value());
            assertEquals(5, streamingAnswers.size());
        }
    }

    private static void setupMockTopicProducer(
            List<Record> streamingAnswers, LangServeInvokeAgent agent) throws Exception {
        TopicProducer topicProducer =
                new TopicProducer() {
                    @Override
                    public CompletableFuture<?> write(Record record) {
                        streamingAnswers.add(record);
                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public long getTotalIn() {
                        return 0;
                    }
                };

        TopicConnectionProvider topicConnectionProvider =
                new TopicConnectionProvider() {
                    @Override
                    public TopicProducer createProducer(
                            String agentId, String topic, Map<String, Object> config) {
                        assertEquals("some-topic", topic);
                        return topicProducer;
                    }
                };

        agent.setContext(
                new AgentContext() {
                    @Override
                    public TopicConsumer getTopicConsumer() {
                        return null;
                    }

                    @Override
                    public TopicProducer getTopicProducer() {
                        return null;
                    }

                    @Override
                    public String getGlobalAgentId() {
                        return null;
                    }

                    @Override
                    public TopicAdmin getTopicAdmin() {
                        return null;
                    }

                    @Override
                    public TopicConnectionProvider getTopicConnectionProvider() {
                        return topicConnectionProvider;
                    }

                    @Override
                    public Path getCodeDirectory() {
                        return null;
                    }
                });
    }
}
