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
package com.datastax.oss.pulsar.functions.transforms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.langstream.api.runner.code.MetricsReporter;
import com.azure.ai.openai.OpenAIAsyncClient;
import com.azure.ai.openai.models.ChatCompletions;
import com.azure.ai.openai.models.ChatCompletionsOptions;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import com.datastax.oss.streaming.ai.services.OpenAIServiceProvider;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Flux;

@Slf4j
public class GenAITest {

    public static Object[][] validConfigs() {
        return new Object[][] {
            {
                "{'steps': [], 'openai': {'access-key': 'qwerty', 'url': 'some-url', 'provider': 'azure'}}"
            },
            {"{'steps': [], 'openai': {'access-key': 'qwerty'}}"},
            {
                "{'steps': [], 'openai': {'access-key': 'qwerty'}, 'huggingface': {'access-key': 'asdf', 'provider': 'api'} }"
            },
            {
                "{'steps': [], 'openai': {'access-key': 'qwerty'}, 'huggingface': {'provider': 'local'} }"
            },
            {
                "{'steps': [{'type': 'compute-ai-embeddings', 'text': '{{ value }}', 'embeddings-field': 'emb', 'model': 'the-new-model'}]}"
            },
            {
                "{'steps': [{'type': 'compute-ai-embeddings', 'text': '{{ value }}', 'embeddings-field': 'emb', 'model': 'ConGen-BERT-Mini'"
                        + ", 'compute-service': 'huggingface', 'model-url': 'jar:///ConGen-BERT-Mini.zip'"
                        + "}],"
                        + " 'openai': {'access-key': 'qwerty'}, 'huggingface': {'provider': 'local'}}"
            },
            {
                "{"
                        + "'steps': ["
                        + "  {"
                        + "    'type': 'ai-chat-completions',"
                        + "    'model': 'example_model',"
                        + "    'messages': ["
                        + "      {"
                        + "        'role': 'user',"
                        + "        'content': 'Hello'"
                        + "      }"
                        + "    ],"
                        + "    'max-tokens': 100,"
                        + "    'temperature': 0.8,"
                        + "    'top-p': 0.9,"
                        + "    'logit-bias': {"
                        + "      'negative': -1"
                        + "    },"
                        + "    'user': 'John',"
                        + "    'stop': ['bye', 'stop'],"
                        + "    'presence-penalty': 0.5,"
                        + "    'frequency-penalty': 0.2"
                        + "  }"
                        + "],"
                        + "'openai': {'access-key': 'qwerty'}"
                        + "}"
            },
            {
                "{'steps': [{'type': 'ai-chat-completions', 'model': 'example_model', 'messages': [{'role': 'user','content': 'Hello'}]}], 'openai': {'access-key': 'qwerty'}}"
            }
        };
    }

    @ParameterizedTest
    @MethodSource("validConfigs")
    void testValidConfig(String validConfig) {
        System.setProperty("ALLOWED_HF_URLS", "jar://");
        log.info("testing valid config: {}", validConfig);
        String userConfig = validConfig.replace("'", "\"");
        Map<String, Object> config =
                new Gson().fromJson(userConfig, new TypeToken<Map<String, Object>>() {}.getType());
        Context context = new Utils.TestContext(null, config);
        TransformFunction transformFunction = new TransformFunction();
        transformFunction.initialize(context);
    }

    public static Object[][] invalidConfigs() {
        return new Object[][] {
            {"{'steps': [], 'openai': {'url': 'some-url'}}"},
            {"{'steps': [], 'openai': {'provider': 'invalid'}}"},
            {
                "{'steps': [{'type': 'compute-ai-embeddings', 'text': '{{ value }}', 'embeddings-field': 'emb'}]}"
            },
            {
                "{'steps': [{'type': 'compute-ai-embeddings', 'text': '{{ value }}', 'model': 'the-new-model'}]}"
            },
            {
                "{'steps': [{'type': 'compute-ai-embeddings', 'embeddings-field': 'emb', 'model': 'the-new-model'}]}"
            },
            {
                "{'steps': [{'type': 'ai-chat-completions', 'model': 'example_model', 'messages': [{'role': 'user','content': 'Hello'}]}]}"
            },
            {
                "{'steps': [{'type': 'ai-chat-completions', 'messages': [{'role': 'user','content': 'Hello'}]}], 'openai': {'access-key': 'qwerty'}}"
            },
            {
                "{'steps': [{'type': 'ai-chat-completions', 'model': 'example_model'}], 'openai': {'access-key': 'qwerty'}}"
            },
            {
                "{'steps': [{'type': 'ai-chat-completions', 'model': 'example_model', 'messages': [{'role': 'invalid','content': 'Hello'}]}], 'openai': {'access-key': 'qwerty'}}"
            },
            {
                "{'steps': [{'type': 'ai-chat-completions', 'model': 'example_model', 'messages': [{'content': 'Hello'}]}], 'openai': {'access-key': 'qwerty'}}"
            }
        };
    }

    @ParameterizedTest
    @MethodSource("invalidConfigs")
    void testInvalidConfig(String invalidConfig) {

        String userConfig = invalidConfig.replace("'", "\"");
        Map<String, Object> config =
                new Gson().fromJson(userConfig, new TypeToken<Map<String, Object>>() {}.getType());
        Context context = new Utils.TestContext(null, config);
        TransformFunction transformFunction = new TransformFunction();
        assertThrows(IllegalArgumentException.class, () -> transformFunction.initialize(context));
    }

    @Test
    void testChatCompletions() throws Exception {
        String userConfig =
                (""
                                + "{"
                                + "  'steps': ["
                                + "    {"
                                + "      'type': 'ai-chat-completions',"
                                + "      'model': 'test-model',"
                                + "      'messages': ["
                                + "        {"
                                + "          'role': 'user',"
                                + "          'content': '{{ value.valueField1 }} {{ key.keyField2 }}'"
                                + "        }"
                                + "      ]"
                                + "    }"
                                + "  ]"
                                + "}")
                        .replace("'", "\"");
        Map<String, Object> config =
                new Gson().fromJson(userConfig, new TypeToken<Map<String, Object>>() {}.getType());
        TransformFunction transformFunction = spy(new TransformFunction());

        OpenAIAsyncClient client = mock(OpenAIAsyncClient.class);

        String completion =
                (""
                                + "{"
                                + "  'choices': ["
                                + "    {"
                                + "      'message': {"
                                + "        'content': 'result',"
                                + "        'role': 'user'"
                                + "      },"
                                + "      'finish_reason': 'stopped'"
                                + "    }"
                                + "  ]"
                                + "}")
                        .replace("'", "\"");
        when(client.getChatCompletionsStream(eq("test-model"), any()))
                .thenAnswer(
                        a ->
                                Flux.just(
                                        new ObjectMapper()
                                                .readValue(completion, ChatCompletions.class)));
        when(transformFunction.buildServiceProvider(any()))
                .thenReturn(new OpenAIServiceProvider(client, MetricsReporter.DISABLED));

        Record<GenericObject> record = Utils.createTestAvroKeyValueRecord();
        Utils.TestContext context = new Utils.TestContext(record, config);
        transformFunction.initialize(context);
        transformFunction.process(record.getValue(), context);

        ArgumentCaptor<ChatCompletionsOptions> captor =
                ArgumentCaptor.forClass(ChatCompletionsOptions.class);
        verify(client).getChatCompletionsStream(eq("test-model"), captor.capture());

        assertEquals(captor.getValue().getMessages().get(0).getContent(), "value1 key2");
    }

    @Test
    void testChatCompletionsWithLogField() throws Exception {
        String userConfig =
                (""
                                + "{"
                                + "  'steps': ["
                                + "    {"
                                + "      'type': 'ai-chat-completions',"
                                + "      'model': 'test-model',"
                                + "      'completion-field': 'value.completion',"
                                + "      'log-field': 'value.log',"
                                + "      'messages': ["
                                + "        {"
                                + "          'role': 'user',"
                                + "          'content': '{{ value.valueField1 }} {{ key.keyField2 }}'"
                                + "        }"
                                + "      ]"
                                + "    }"
                                + "  ]"
                                + "}")
                        .replace("'", "\"");
        Map<String, Object> config =
                new Gson().fromJson(userConfig, new TypeToken<Map<String, Object>>() {}.getType());
        TransformFunction transformFunction = spy(new TransformFunction());

        OpenAIAsyncClient client = mock(OpenAIAsyncClient.class);

        String completion =
                (""
                                + "{"
                                + "  'choices': ["
                                + "    {"
                                + "      'message': {"
                                + "        'content': 'result',"
                                + "        'role': 'user'"
                                + "      },"
                                + "      'finish_reason': 'stopped'"
                                + "    }"
                                + "  ]"
                                + "}")
                        .replace("'", "\"");
        when(client.getChatCompletionsStream(eq("test-model"), any()))
                .thenAnswer(
                        a ->
                                Flux.just(
                                        new ObjectMapper()
                                                .readValue(completion, ChatCompletions.class)));
        when(transformFunction.buildServiceProvider(any()))
                .thenReturn(new OpenAIServiceProvider(client, MetricsReporter.DISABLED));

        Record<GenericObject> record = Utils.createTestAvroKeyValueRecord();
        Utils.TestContext context = new Utils.TestContext(record, config);
        transformFunction.initialize(context);
        Record<?> outputRecord = transformFunction.process(record.getValue(), context);

        KeyValueSchema<?, ?> messageSchema = (KeyValueSchema<?, ?>) outputRecord.getSchema();
        KeyValue<?, ?> messageValue = (KeyValue<?, ?>) outputRecord.getValue();
        GenericData.Record valueAvroRecord =
                Utils.getRecord(messageSchema.getValueSchema(), (byte[]) messageValue.getValue());
        assertEquals("result", valueAvroRecord.get("completion").toString());
        assertEquals(
                "{\"options\":{\"type\":\"ai-chat-completions\",\"when\":null,\"model\":\"test-model\","
                        + "\"messages\":[{\"role\":\"user\",\"content\":\"{{ value.valueField1 }} {{ key.keyField2 }}\"}],"
                        + "\"stream-to-topic\":null,\"stream-response-completion-field\":null,\"min-chunks-per-message\":20,"
                        + "\"completion-field\":\"value.completion\",\"stream\":true,\"log-field\":\"value.log\","
                        + "\"max-tokens\":null,\"temperature\":null,\"top-p\":null,\"logit-bias\":null,\"user\":null,"
                        + "\"stop\":null,\"presence-penalty\":null,\"frequency-penalty\":null,\"options\":null},"
                        + "\"messages\":[{\"role\":\"user\",\"content\":\"value1 key2\"}],\"model\":\"test-model\"}",
                valueAvroRecord.get("log").toString());
    }

    @Test
    void testQuery() throws Exception {
        String userConfig =
                (""
                                + "{'datasource': {'service': 'mock','username': 'test','password': 'testpwd', 'secureBundle':'xx'},"
                                + "   'steps': ["
                                + "    {'type': 'query', 'fields': ['key.keyField1'], 'query':'select * from products where description like ?', 'output-field':'value.results'}"
                                + "]}")
                        .replace("'", "\"");
        Map<String, Object> config =
                new Gson().fromJson(userConfig, new TypeToken<Map<String, Object>>() {}.getType());
        TransformFunction transformFunction =
                new TransformFunction() {
                    @Override
                    protected QueryStepDataSource buildDataSource(
                            Map<String, Object> dataSourceConfig) {
                        assertEquals(dataSourceConfig.get("service"), "mock");
                        assertEquals(dataSourceConfig.get("username"), "test");
                        assertEquals(dataSourceConfig.get("password"), "testpwd");
                        assertEquals(dataSourceConfig.get("secureBundle"), "xx");

                        return new QueryStepDataSource() {
                            @Override
                            public List<Map<String, Object>> fetchData(
                                    String query, List<Object> params) {
                                assertEquals(
                                        "select * from products where description like ?", query);
                                assertEquals(params.size(), 1);
                                assertEquals(params.get(0), "key1");

                                return List.of(
                                        Map.of("productId", "1", "name", "Product1"),
                                        Map.of("productId", "2", "name", "Product2"));
                            }
                        };
                    }
                };

        Record<GenericObject> record = Utils.createTestAvroKeyValueRecord();
        Utils.TestContext context = new Utils.TestContext(record, config);
        transformFunction.initialize(context);
        Record<?> outputRecord = transformFunction.process(record.getValue(), context);

        KeyValueSchema<?, ?> messageSchema = (KeyValueSchema<?, ?>) outputRecord.getSchema();
        KeyValue<?, ?> messageValue = (KeyValue<?, ?>) outputRecord.getValue();

        GenericData.Record valueAvroRecord =
                Utils.getRecord(messageSchema.getValueSchema(), (byte[]) messageValue.getValue());
        Object results = valueAvroRecord.get("results");
        assertNotNull(results);
        GenericArray array = (GenericArray) results;
        assertEquals(2, array.size());
        assertEquals(
                Map.of(
                        new Utf8("productId"),
                        new Utf8("1"),
                        new Utf8("name"),
                        new Utf8("Product1")),
                array.get(0));
        assertEquals(
                Map.of(
                        new Utf8("productId"),
                        new Utf8("2"),
                        new Utf8("name"),
                        new Utf8("Product2")),
                array.get(1));
    }
}
