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
package com.datastax.oss.streaming.ai.util;

import com.azure.ai.openai.OpenAIClient;
import com.azure.ai.openai.OpenAIClientBuilder;
import com.azure.ai.openai.models.NonAzureOpenAIKeyCredential;
import com.azure.core.credential.AzureKeyCredential;
import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpPipeline;
import com.azure.core.http.HttpPipelineBuilder;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.datastax.oss.streaming.ai.CastStep;
import com.datastax.oss.streaming.ai.ChatCompletionsStep;
import com.datastax.oss.streaming.ai.ComputeAIEmbeddingsStep;
import com.datastax.oss.streaming.ai.ComputeStep;
import com.datastax.oss.streaming.ai.DropFieldStep;
import com.datastax.oss.streaming.ai.DropStep;
import com.datastax.oss.streaming.ai.FlattenStep;
import com.datastax.oss.streaming.ai.MergeKeyValueStep;
import com.datastax.oss.streaming.ai.QueryStep;
import com.datastax.oss.streaming.ai.TransformContext;
import com.datastax.oss.streaming.ai.TransformStep;
import com.datastax.oss.streaming.ai.UnwrapKeyValueStep;
import com.datastax.oss.streaming.ai.completions.CompletionsService;
import com.datastax.oss.streaming.ai.datasource.AstraDBDataSource;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import com.datastax.oss.streaming.ai.embeddings.EmbeddingsService;
import com.datastax.oss.streaming.ai.jstl.predicate.JstlPredicate;
import com.datastax.oss.streaming.ai.jstl.predicate.StepPredicatePair;
import com.datastax.oss.streaming.ai.model.ComputeField;
import com.datastax.oss.streaming.ai.model.ComputeFieldType;
import com.datastax.oss.streaming.ai.model.TransformSchemaType;
import com.datastax.oss.streaming.ai.model.config.CastConfig;
import com.datastax.oss.streaming.ai.model.config.ChatCompletionsConfig;
import com.datastax.oss.streaming.ai.model.config.ComputeAIEmbeddingsConfig;
import com.datastax.oss.streaming.ai.model.config.ComputeConfig;
import com.datastax.oss.streaming.ai.model.config.DropFieldsConfig;
import com.datastax.oss.streaming.ai.model.config.FlattenConfig;
import com.datastax.oss.streaming.ai.model.config.OpenAIConfig;
import com.datastax.oss.streaming.ai.model.config.OpenAIProvider;
import com.datastax.oss.streaming.ai.model.config.QueryConfig;
import com.datastax.oss.streaming.ai.model.config.StepConfig;
import com.datastax.oss.streaming.ai.model.config.TransformStepConfig;
import com.datastax.oss.streaming.ai.model.config.UnwrapKeyValueConfig;
import com.datastax.oss.streaming.ai.services.ServiceProvider;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
public class TransformFunctionUtil {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final List<String> FIELD_NAMES =
            Arrays.asList(
                    "value", "key", "destinationTopic", "messageKey", "topicName", "eventTime");

    public static OpenAIClient buildOpenAIClient(OpenAIConfig openAIConfig) {
        if (openAIConfig == null) {
            return null;
        }
        OpenAIClientBuilder openAIClientBuilder = new OpenAIClientBuilder();
        if (openAIConfig.getProvider() == OpenAIProvider.AZURE) {
            openAIClientBuilder.credential(new AzureKeyCredential(openAIConfig.getAccessKey()));
        } else {
            openAIClientBuilder.credential(
                    new NonAzureOpenAIKeyCredential(openAIConfig.getAccessKey()));
        }
        if (openAIConfig.getUrl() != null) {
            openAIClientBuilder.endpoint(openAIConfig.getUrl());

            // this is for testing only
            if (openAIConfig.getUrl().startsWith("http://localhost")) {
                HttpPipeline httpPipeline =
                        new HttpPipelineBuilder()
                                .httpClient(new MockHttpClient(openAIConfig.getAccessKey()))
                                .build();
                openAIClientBuilder.pipeline(httpPipeline);
            }
        }

        return openAIClientBuilder.buildClient();
    }

    public static QueryStepDataSource buildDataSource(Map<String, Object> dataSourceConfig) {
        if (dataSourceConfig == null) {
            return new QueryStepDataSource() {};
        }
        QueryStepDataSource dataSource;
        String service = (String) dataSourceConfig.get("service");
        switch (service) {
            case "astra":
                dataSource = new AstraDBDataSource();
                break;
            default:
                throw new IllegalArgumentException(
                        "Invalid service type " + service);
        }
        dataSource.initialize(dataSourceConfig);
        return dataSource;
    }

    public static StepPredicatePair buildStep(
            TransformStepConfig transformConfig,
            ServiceProvider serviceProvider,
            QueryStepDataSource dataSource,
            ChatCompletionsStep.StreamingAnswersConsumerFactory streamingAnswersConsumerFactory,
            StepConfig step)
            throws Exception {
        TransformStep transformStep;
        switch (step.getType()) {
            case "drop-fields":
                transformStep = newRemoveFieldFunction((DropFieldsConfig) step);
                break;
            case "cast":
                transformStep =
                        newCastFunction(
                                (CastConfig) step, transformConfig.isAttemptJsonConversion());
                break;
            case "merge-key-value":
                transformStep = new MergeKeyValueStep();
                break;
            case "unwrap-key-value":
                transformStep = newUnwrapKeyValueFunction((UnwrapKeyValueConfig) step);
                break;
            case "flatten":
                transformStep = newFlattenFunction((FlattenConfig) step);
                break;
            case "drop":
                transformStep = new DropStep();
                break;
            case "compute":
                transformStep = newComputeFieldFunction((ComputeConfig) step);
                break;
            case "compute-ai-embeddings":
                transformStep =
                        newComputeAIEmbeddings((ComputeAIEmbeddingsConfig) step, serviceProvider);
                break;
            case "ai-chat-completions":
                transformStep =
                        newChatCompletionsFunction(
                                (ChatCompletionsConfig) step,
                                serviceProvider,
                                streamingAnswersConsumerFactory);
                break;
            case "query":
                transformStep = newQuery((QueryConfig) step, dataSource);
                break;
            default:
                throw new IllegalArgumentException("Invalid step type: " + step.getType());
        }
        return new StepPredicatePair(
                transformStep, step.getWhen() == null ? null : new JstlPredicate(step.getWhen()));
    }

    public static DropFieldStep newRemoveFieldFunction(DropFieldsConfig config) {
        DropFieldStep.DropFieldStepBuilder builder = DropFieldStep.builder();
        if (config.getPart() != null) {
            if (config.getPart().equals("key")) {
                builder.keyFields(config.getFields());
            } else {
                builder.valueFields(config.getFields());
            }
        } else {
            builder.keyFields(config.getFields()).valueFields(config.getFields());
        }
        return builder.build();
    }

    public static CastStep newCastFunction(CastConfig config, boolean attemptJsonConversion) {
        String schemaTypeParam = config.getSchemaType();
        TransformSchemaType schemaType = TransformSchemaType.valueOf(schemaTypeParam);
        CastStep.CastStepBuilder builder =
                CastStep.builder().attemptJsonConversion(attemptJsonConversion);
        if (config.getPart() != null) {
            if (config.getPart().equals("key")) {
                builder.keySchemaType(schemaType);
            } else {
                builder.valueSchemaType(schemaType);
            }
        } else {
            builder.keySchemaType(schemaType).valueSchemaType(schemaType);
        }
        return builder.build();
    }

    public static FlattenStep newFlattenFunction(FlattenConfig config) {
        FlattenStep.FlattenStepBuilder builder = FlattenStep.builder();
        if (config.getPart() != null) {
            builder.part(config.getPart());
        }
        if (config.getDelimiter() != null) {
            builder.delimiter(config.getDelimiter());
        }
        return builder.build();
    }

    public static TransformStep newComputeFieldFunction(ComputeConfig config) {
        List<ComputeField> fieldList = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        config.getFields()
                .forEach(
                        field -> {
                            if (seen.contains(field.getName())) {
                                throw new IllegalArgumentException(
                                        "Duplicate compute field name detected: "
                                                + field.getName());
                            }
                            if (field.getType() == ComputeFieldType.DATE
                                    && ("value".equals(field.getName())
                                            || "key".equals(field.getName()))) {
                                throw new IllegalArgumentException(
                                        "The compute operation cannot apply the type DATE to the message value or key. "
                                                + "Please consider using the types TIMESTAMP or INSTANT instead and follow with a 'cast' "
                                                + "to SchemaType.DATE operation.");
                            }
                            seen.add(field.getName());
                            ComputeFieldType type =
                                    "destinationTopic".equals(field.getName())
                                                    || "messageKey".equals(field.getName())
                                                    || field.getName().startsWith("properties.")
                                            ? ComputeFieldType.STRING
                                            : field.getType();
                            fieldList.add(
                                    ComputeField.builder()
                                            .scopedName(field.getName())
                                            .expression(field.getExpression())
                                            .type(type)
                                            .optional(field.isOptional())
                                            .build());
                        });
        return ComputeStep.builder().fields(fieldList).build();
    }

    @SneakyThrows
    public static TransformStep newComputeAIEmbeddings(
            ComputeAIEmbeddingsConfig config, ServiceProvider provider) {
        EmbeddingsService embeddingsService = provider.getEmbeddingsService(convertToMap(config));
        return new ComputeAIEmbeddingsStep(
                config.getText(),
                config.getEmbeddingsFieldName(),
                config.getBatchSize(),
                config.getFlushInterval(),
                embeddingsService);
    }

    public static UnwrapKeyValueStep newUnwrapKeyValueFunction(UnwrapKeyValueConfig config) {
        return new UnwrapKeyValueStep(config.isUnwrapKey());
    }

    public static Map<String, Object> convertToMap(Object object) {
        return new ObjectMapper().convertValue(object, Map.class);
    }

    public static <T> T convertFromMap(Map<String, Object> map, Class<T> type) {
        return new ObjectMapper().convertValue(map, type);
    }

    public static ChatCompletionsStep newChatCompletionsFunction(
            ChatCompletionsConfig config,
            ServiceProvider serviceProvider,
            ChatCompletionsStep.StreamingAnswersConsumerFactory streamingAnswersConsumerFactory)
            throws Exception {
        CompletionsService completionsService =
                serviceProvider.getCompletionsService(convertToMap(config));
        return new ChatCompletionsStep(completionsService, streamingAnswersConsumerFactory, config);
    }

    public static TransformStep newQuery(QueryConfig config, QueryStepDataSource dataSource) {
        config.getFields()
                .forEach(
                        field -> {
                            if (!FIELD_NAMES.contains(field)
                                    && !field.startsWith("value.")
                                    && !field.startsWith("key.")
                                    && !field.startsWith("properties")) {
                                throw new IllegalArgumentException(
                                        String.format(
                                                "Invalid field name for query step: %s", field));
                            }
                        });
        return QueryStep.builder()
                .outputFieldName(config.getOutputField())
                .query(config.getQuery())
                .onlyFirst(config.isOnlyFirst())
                .fields(config.getFields())
                .dataSource(dataSource)
                .build();
    }

    public static void processTransformSteps(
            TransformContext transformContext, Collection<StepPredicatePair> steps)
            throws Exception {
        for (StepPredicatePair pair : steps) {
            processStep(transformContext, pair);
        }
    }

    public static void processStep(TransformContext transformContext, StepPredicatePair pair)
            throws Exception {
        TransformStep step = pair.getTransformStep();
        Predicate<TransformContext> predicate = pair.getPredicate();
        if (predicate == null || predicate.test(transformContext)) {
            step.process(transformContext);
        }
    }

    public static Object attemptJsonConversion(Object value) {
        try {
            if (value instanceof String) {
                return OBJECT_MAPPER.readValue(
                        (String) value, new TypeReference<Map<String, Object>>() {});
            } else if (value instanceof byte[]) {
                return OBJECT_MAPPER.readValue(
                        (byte[]) value, new TypeReference<Map<String, Object>>() {});
            }
        } catch (IOException e) {
            if (log.isDebugEnabled()) {
                log.debug("Cannot convert value to json", e);
            }
        }
        return value;
    }

    public static byte[] getBytes(ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            return null;
        }
        if (byteBuffer.hasArray()
                && byteBuffer.arrayOffset() == 0
                && byteBuffer.array().length == byteBuffer.remaining()) {
            return byteBuffer.array();
        }
        // Direct buffer is not backed by array and it needs to be read from direct memory
        byte[] array = new byte[byteBuffer.remaining()];
        byteBuffer.get(array);
        return array;
    }

    public static Object safeClone(Object object) {
        if (object == null) {
            return null;
        }
        if (object.getClass().isPrimitive()
                || object instanceof String
                || object instanceof Number
                || object instanceof Boolean) {
            return object;
        }
        if (object instanceof Map map) {
            HashMap<Object, Object> res = new HashMap<>();
            map.forEach((k, v) -> res.put(safeClone(k), safeClone(v)));
            return res;
        }
        if (object instanceof List list) {
            List<Object> res = new ArrayList<>();
            list.forEach(v -> res.add(safeClone(v)));
            return res;
        }
        if (object instanceof Set set) {
            Set<Object> res = new HashSet<>();
            set.forEach(v -> res.add(safeClone(v)));
            return res;
        }
        if (object instanceof GenericRecord genericRecord) {
            return GenericData.get().deepCopy(genericRecord.getSchema(), genericRecord);
        }
        if (object instanceof JsonNode jsonNode) {
            return jsonNode.deepCopy();
        }
        throw new UnsupportedOperationException("Cannot copy a value of " + object.getClass());
    }

    private static class MockHttpClient implements HttpClient {

        private final java.net.http.HttpClient httpClient;
        private String apiKey;

        @SneakyThrows
        public MockHttpClient(String apiKey) {
            this.apiKey = apiKey;
            SSLContext mockSslContext = SSLContext.getInstance("TLS");
            TrustManager[] trustManagerArray = {new NullX509TrustManager()};
            mockSslContext.init(null, trustManagerArray, new SecureRandom());
            httpClient =
                    java.net.http.HttpClient.newBuilder()
                            .sslParameters(mockSslContext.getDefaultSSLParameters())
                            .sslContext(mockSslContext)
                            .build();
        }

        @Override
        public Mono<HttpResponse> send(HttpRequest httpRequest) {
            byte[] body = httpRequest.getBodyAsBinaryData().toBytes();

            try {
                java.net.http.HttpRequest.Builder builder =
                        java.net.http.HttpRequest.newBuilder()
                                .uri(httpRequest.getUrl().toURI())
                                .method(
                                        httpRequest.getHttpMethod().name(),
                                        java.net.http.HttpRequest.BodyPublishers.ofByteArray(body));

                httpRequest
                        .getHeaders()
                        .forEach(
                                (header) -> {
                                    log.info(
                                            "Proxy header {}: {}",
                                            header.getName(),
                                            header.getValue());
                                    switch (header.getName()) {
                                        case "Content-Length":
                                            break;
                                        default:
                                            builder.header(header.getName(), header.getValue());
                                    }
                                });
                if (apiKey != null) {
                    builder.header("api-key", apiKey);
                }
                return Mono.fromFuture(
                        httpClient
                                .sendAsync(
                                        builder.build(),
                                        java.net.http.HttpResponse.BodyHandlers.ofString())
                                .thenApply(
                                        (response) -> {
                                            log.info("Response: {}", response.body());
                                            return new MyHttpResponse(httpRequest, response);
                                        }));
            } catch (Exception e) {
                return Mono.fromFuture(CompletableFuture.failedFuture(e));
            }
        }

        private static class MyHttpResponse extends HttpResponse {

            private java.net.http.HttpResponse<String> response;

            public MyHttpResponse(
                    HttpRequest httpRequest, java.net.http.HttpResponse<String> response) {
                super(httpRequest);
                this.response = response;
            }

            @Override
            public int getStatusCode() {
                return response.statusCode();
            }

            @Override
            public String getHeaderValue(String s) {
                return response.headers().firstValue(s).orElse(null);
            }

            @Override
            public HttpHeaders getHeaders() {
                HttpHeaders result = new HttpHeaders();
                response.headers().map().forEach((k, v) -> result.set(k, v));
                return result;
            }

            @Override
            public Flux<ByteBuffer> getBody() {
                ByteBuffer buffer =
                        ByteBuffer.wrap(response.body().getBytes(StandardCharsets.UTF_8));
                return Flux.fromIterable(List.of(buffer));
            }

            @Override
            public Mono<byte[]> getBodyAsByteArray() {
                return Mono.fromFuture(
                        CompletableFuture.completedFuture(
                                response.body().getBytes(StandardCharsets.UTF_8)));
            }

            @Override
            public Mono<String> getBodyAsString() {
                return Mono.fromFuture(CompletableFuture.completedFuture(response.body()));
            }

            @Override
            public Mono<String> getBodyAsString(Charset charset) {
                return Mono.fromFuture(CompletableFuture.completedFuture(response.body()));
            }
        }

        private static class NullX509TrustManager implements X509TrustManager {
            public void checkClientTrusted(X509Certificate[] chain, String authType) {
                // do nothing
            }

            public void checkServerTrusted(X509Certificate[] chain, String authType) {
                // do nothing
            }

            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }
        }
    }

    /**
     * Aggregate records in batches, depending on a batch size and a maximum idle time.
     *
     * @param <T>
     */
    public static class BatchExecutor<T> {
        private final int batchSize;
        private List<T> batch;
        private long flushInterval;
        private ScheduledExecutorService scheduledExecutorService;

        private ScheduledFuture<?> scheduledFuture;

        private final Consumer<List<T>> processor;

        public BatchExecutor(
                int batchSize,
                Consumer<List<T>> processor,
                long maxIdleTime,
                ScheduledExecutorService scheduledExecutorService) {
            this.batchSize = batchSize;
            this.batch = new ArrayList<>(batchSize);
            this.processor = processor;
            this.flushInterval = maxIdleTime;
            this.scheduledExecutorService = scheduledExecutorService;
        }

        public void start() {
            if (flushInterval > 0) {
                scheduledFuture =
                        scheduledExecutorService.scheduleWithFixedDelay(
                                this::flush, flushInterval, flushInterval, TimeUnit.MILLISECONDS);
            }
        }

        public void stop() {
            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
            }
            flush();
        }

        private void flush() {
            List<T> batchToProcess = null;
            synchronized (this) {
                if (batch.isEmpty()) {
                    return;
                }
                if (!batch.isEmpty()) {
                    batchToProcess = batch;
                    batch = new ArrayList<>(batchSize);
                }
            }
            // execute the processor our of the synchronized block
            processor.accept(batchToProcess);
        }

        public void add(T t) {
            List<T> batchToProcess = null;
            synchronized (this) {
                batch.add(t);
                if (batch.size() >= batchSize || flushInterval <= 0) {
                    batchToProcess = batch;
                    batch = new ArrayList<>(batchSize);
                }
            }

            // execute the processor our of the synchronized block
            if (batchToProcess != null) {
                processor.accept(batchToProcess);
            }
        }
    }
}
