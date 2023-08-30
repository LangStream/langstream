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
import com.datastax.oss.streaming.ai.model.config.DataSourceConfig;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

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
        }
        return openAIClientBuilder.buildClient();
    }

    public static QueryStepDataSource buildDataSource(DataSourceConfig dataSourceConfig) {
        if (dataSourceConfig == null) {
            return new QueryStepDataSource() {};
        }
        QueryStepDataSource dataSource;
        switch (dataSourceConfig.getService() + "") {
            case "astra":
                dataSource = new AstraDBDataSource();
                break;
            default:
                throw new IllegalArgumentException(
                        "Invalid service type " + dataSourceConfig.getService());
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
                config.getText(), config.getEmbeddingsFieldName(), embeddingsService);
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

    public static Double getDouble(String name, Map<String, Object> options) {
        Object o = options.get(name);
        if (o == null) {
            return null;
        }
        if (o instanceof Number) {
            return ((Number) o).doubleValue();
        }
        return Double.parseDouble(o.toString());
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
        if (object.getClass().isPrimitive()) {
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
}
