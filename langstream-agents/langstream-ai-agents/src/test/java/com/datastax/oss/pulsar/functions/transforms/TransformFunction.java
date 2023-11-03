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

import static ai.langstream.ai.agents.commons.MutableRecord.attemptJsonConversion;
import static com.datastax.oss.streaming.ai.util.TransformFunctionUtil.buildStep;

import ai.langstream.ai.agents.commons.MutableRecord;
import ai.langstream.ai.agents.commons.TransformSchemaType;
import ai.langstream.ai.agents.services.impl.HuggingFaceProvider;
import ai.langstream.api.runner.code.MetricsReporter;
import com.datastax.oss.streaming.ai.StepPredicatePair;
import com.datastax.oss.streaming.ai.TransformStep;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import com.datastax.oss.streaming.ai.model.config.StepConfig;
import com.datastax.oss.streaming.ai.model.config.TransformStepConfig;
import com.datastax.oss.streaming.ai.services.OpenAIServiceProvider;
import com.datastax.oss.streaming.ai.services.ServiceProvider;
import com.datastax.oss.streaming.ai.util.TransformFunctionUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SchemaValidatorsConfig;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import com.networknt.schema.urn.URNFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.api.utils.FunctionRecord;

/**
 * <code>TransformFunction</code> is a {@link Function} that provides an easy way to apply a set of
 * usual basic transformations to the data.
 *
 * <p>It provides the following transformations:
 *
 * <ul>
 *   <li><code>cast</code>: modifies the key or value schema to a target compatible schema passed in
 *       the <code>schema-type</code> argument. This PR only enables <code>STRING</code>
 *       schema-type. The <code>part</code> argument allows to choose on which part to apply between
 *       <code>key</code> and <code>value</code>. If <code>part</code> is null or absent the
 *       transformations applies to both the key and value.
 *   <li><code>drop-fields</code>: drops fields given as a string list in parameter <code>fields
 *       </code>. The <code>part</code> argument allows to choose on which part to apply between
 *       <code>key</code> and <code>value</code>. If <code>part</code> is null or absent the
 *       transformations applies to both the key and value. Currently only AVRO is supported.
 *   <li><code>merge-key-value</code>: merges the fields of KeyValue records where both the key and
 *       value are structured types of the same schema type. Currently only AVRO is supported.
 *   <li><code>unwrap-key-value</code>: if the record is a KeyValue, extract the KeyValue's value
 *       and make it the record value. If parameter <code>unwrapKey</code> is present and set to
 *       <code>true</code>, extract the KeyValue's key instead.
 *   <li><code>flatten</code>: flattens a nested structure selected in the <code>part</code> by
 *       concatenating nested field names with a <code>delimiter</code> and populating them as top
 *       level fields. <code>
 *       delimiter</code> defaults to '_'. <code>part</code> could be any of <code>key</code> or
 *       <code>value</code>. If not specified, flatten will apply to key and value.
 *   <li><code>drop</code>: drops the message from further processing. Use in conjunction with
 *       <code>when</code> to selectively drop messages.
 *   <li><code>compute</code>: dynamically calculates <code>fields</code> values in the key, value
 *       or header. Each field has a <code>name</code> to represents a new or existing field (in
 *       this case, it will be overwritten). The value of the fields is evaluated by the <code>
 *       expression</code> and respect the <code>type</code>. Supported types are [INT32, INT64,
 *       FLOAT, DOUBLE, BOOLEAN, DATE, TIME, DATETIME]. Each field is marked as nullable by default.
 *       To mark the field as non-nullable in the output schema, set <code>optional</code> to false.
 * </ul>
 *
 * <p>The <code>TransformFunction</code> reads its configuration as Json from the {@link Context}
 * <code>userConfig</code> in the format:
 *
 * <pre><code class="lang-json">
 * {
 *   "steps": [
 *     {
 *       "type": "cast", "schema-type": "STRING"
 *     },
 *     {
 *       "type": "drop-fields", "fields": ["keyField1", "keyField2"], "part": "key"
 *     },
 *     {
 *       "type": "merge-key-value"
 *     },
 *     {
 *       "type": "unwrap-key-value"
 *     },
 *     {
 *       "type": "flatten", "delimiter" : "_" "part" : "value", "when": "value.field == 'value'"
 *     },
 *     {
 *       "type": "drop", "when": "value.field == 'value'"
 *     },
 *     {
 *       "type": "compute", "fields": [{"name": "value.new-field", "expression": "key.existing-field == 'value'", "type": "BOOLEAN"}]
 *     }
 *   ]
 * }
 * </code></pre>
 *
 * @see <a href="https://github.com/apache/pulsar/issues/15902">PIP-173 : Create a built-in Function
 *     implementing the most common basic transformations</a>
 */
@Slf4j
public class TransformFunction
        implements Function<GenericObject, Record<GenericObject>>, TransformStep {
    private List<StepPredicatePair> steps;
    private TransformStepConfig transformConfig;
    private QueryStepDataSource dataSource;
    private ServiceProvider serviceProvider;

    @Override
    @SneakyThrows
    public void initialize(Context context) {
        Map<String, Object> userConfigMap = context.getUserConfigMap();
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        JsonNode jsonNode = mapper.convertValue(userConfigMap, JsonNode.class);

        URNFactory urnFactory =
                urn -> {
                    try {
                        URL absoluteURL =
                                Thread.currentThread().getContextClassLoader().getResource(urn);
                        return absoluteURL.toURI();
                    } catch (Exception ex) {
                        return null;
                    }
                };
        JsonSchemaFactory factory =
                JsonSchemaFactory.builder(JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V4))
                        .objectMapper(mapper)
                        .addUrnFactory(urnFactory)
                        .build();
        SchemaValidatorsConfig jsonSchemaConfig = new SchemaValidatorsConfig();
        jsonSchemaConfig.setLosslessNarrowing(true);
        InputStream is =
                Thread.currentThread()
                        .getContextClassLoader()
                        .getResourceAsStream("config-schema.yaml");

        JsonSchema schema = factory.getSchema(is, jsonSchemaConfig);
        Set<ValidationMessage> errors = schema.validate(jsonNode);

        if (errors.size() != 0) {
            if (!jsonNode.hasNonNull("steps")) {
                throw new IllegalArgumentException("Missing config 'steps' field");
            }
            JsonNode steps = jsonNode.get("steps");
            if (!steps.isArray()) {
                throw new IllegalArgumentException("Config 'steps' field must be an array");
            }
            String errorMessage = null;
            try {
                for (JsonNode step : steps) {
                    String type = step.get("type").asText();
                    JsonSchema stepSchema =
                            factory.getSchema(
                                    String.format(
                                            "{\"$ref\": \"config-schema.yaml#/components/schemas/%s\"}",
                                            kebabToPascal(type)));

                    errorMessage =
                            stepSchema.validate(step).stream()
                                    .findFirst()
                                    .map(
                                            v ->
                                                    String.format(
                                                            "Invalid '%s' step config: %s",
                                                            type, v))
                                    .orElse(null);
                    if (errorMessage != null) {
                        break;
                    }
                }
            } catch (Exception e) {
                log.debug("Exception during steps validation, ignoring", e);
            }

            if (errorMessage != null) {
                throw new IllegalArgumentException(errorMessage);
            }

            errors.stream()
                    .findFirst()
                    .ifPresent(
                            validationMessage -> {
                                throw new IllegalArgumentException(
                                        "Configuration validation failed: " + validationMessage);
                            });
        }

        transformConfig = mapper.convertValue(userConfigMap, TransformStepConfig.class);

        serviceProvider = buildServiceProvider(transformConfig);
        dataSource = buildDataSource(transformConfig.getDatasource());

        steps = getTransformSteps(transformConfig, serviceProvider, dataSource);

        for (StepPredicatePair pair : steps) {
            pair.getTransformStep().start();
        }
    }

    public static List<StepPredicatePair> getTransformSteps(
            TransformStepConfig transformConfig,
            ServiceProvider serviceProvider,
            QueryStepDataSource dataSource)
            throws Exception {
        List<StepPredicatePair> steps = new ArrayList<>();
        for (StepConfig step : transformConfig.getSteps()) {
            steps.add(buildStep(transformConfig, serviceProvider, dataSource, null, step));
        }
        return steps;
    }

    @Override
    public void close() throws Exception {
        if (dataSource != null) {
            dataSource.close();
        }
        if (serviceProvider != null) {
            serviceProvider.close();
        }
        for (StepPredicatePair pair : steps) {
            pair.getTransformStep().close();
        }
    }

    @Override
    public Record<GenericObject> process(GenericObject input, Context context) throws Exception {
        Object nativeObject = input.getNativeObject();
        if (log.isDebugEnabled()) {
            Record<?> currentRecord = context.getCurrentRecord();
            log.debug("apply to {} {}", input, nativeObject);
            log.debug(
                    "record with schema {} version {} {}",
                    currentRecord.getSchema(),
                    currentRecord.getMessage().orElseThrow().getSchemaVersion(),
                    currentRecord);
        }

        MutableRecord mutableRecord =
                newTransformContext(
                        context, nativeObject, transformConfig.isAttemptJsonConversion());
        process(mutableRecord);
        return send(context, mutableRecord);
    }

    public static MutableRecord newTransformContext(
            Context context, Object value, boolean attemptJsonConversion) {
        Record<?> currentRecord = context.getCurrentRecord();
        MutableRecord mutableRecord = new MutableRecord();
        mutableRecord.setProperties(new HashMap<>());
        mutableRecord.setInputTopic(currentRecord.getTopicName().orElse(null));
        mutableRecord.setOutputTopic(currentRecord.getDestinationTopic().orElse(null));
        mutableRecord.setKey(currentRecord.getKey().orElse(null));
        mutableRecord.setEventTime(currentRecord.getEventTime().orElse(null));

        if (currentRecord.getProperties() != null) {
            mutableRecord.setProperties(new HashMap<>(currentRecord.getProperties()));
        }

        Schema<?> schema = currentRecord.getSchema();
        if (schema instanceof KeyValueSchema && value instanceof KeyValue) {
            KeyValueSchema<?, ?> kvSchema = (KeyValueSchema<?, ?>) schema;
            KeyValue<?, ?> kv = (KeyValue<?, ?>) value;
            Schema<?> keySchema = kvSchema.getKeySchema();
            Schema<?> valueSchema = kvSchema.getValueSchema();
            mutableRecord.setKeySchemaType(pulsarSchemaToTransformSchemaType(keySchema));
            mutableRecord.setKeyNativeSchema(getNativeSchema(keySchema));
            mutableRecord.setKeyObject(
                    keySchema.getSchemaInfo().getType().isStruct()
                            ? ((GenericObject) kv.getKey()).getNativeObject()
                            : kv.getKey());
            mutableRecord.setValueSchemaType(pulsarSchemaToTransformSchemaType(valueSchema));
            mutableRecord.setValueNativeSchema(getNativeSchema(valueSchema));
            mutableRecord.setValueObject(
                    valueSchema.getSchemaInfo().getType().isStruct()
                            ? ((GenericObject) kv.getValue()).getNativeObject()
                            : kv.getValue());
            mutableRecord
                    .getCustomContext()
                    .put("keyValueEncodingType", kvSchema.getKeyValueEncodingType());
        } else {
            mutableRecord.setValueSchemaType(pulsarSchemaToTransformSchemaType(schema));
            mutableRecord.setValueNativeSchema(getNativeSchema(schema));
            mutableRecord.setValueObject(value);
        }
        if (attemptJsonConversion) {
            mutableRecord.setKeyObject(attemptJsonConversion(mutableRecord.getKeyObject()));
            mutableRecord.setValueObject(attemptJsonConversion(mutableRecord.getValueObject()));
        }
        return mutableRecord;
    }

    private static Object getNativeSchema(Schema<?> schema) {
        if (schema == null) {
            return null;
        }
        return schema.getNativeSchema().orElse(null);
    }

    private static TransformSchemaType pulsarSchemaToTransformSchemaType(Schema<?> schema) {
        if (schema == null) {
            return null;
        }
        switch (schema.getSchemaInfo().getType()) {
            case INT8:
                return TransformSchemaType.INT8;
            case INT16:
                return TransformSchemaType.INT16;
            case INT32:
                return TransformSchemaType.INT32;
            case INT64:
                return TransformSchemaType.INT64;
            case FLOAT:
                return TransformSchemaType.FLOAT;
            case DOUBLE:
                return TransformSchemaType.DOUBLE;
            case BOOLEAN:
                return TransformSchemaType.BOOLEAN;
            case STRING:
                return TransformSchemaType.STRING;
            case BYTES:
                return TransformSchemaType.BYTES;
            case DATE:
                return TransformSchemaType.DATE;
            case TIME:
                return TransformSchemaType.TIME;
            case TIMESTAMP:
                return TransformSchemaType.TIMESTAMP;
            case INSTANT:
                return TransformSchemaType.INSTANT;
            case LOCAL_DATE:
                return TransformSchemaType.LOCAL_DATE;
            case LOCAL_TIME:
                return TransformSchemaType.LOCAL_TIME;
            case LOCAL_DATE_TIME:
                return TransformSchemaType.LOCAL_DATE_TIME;
            case JSON:
                return TransformSchemaType.JSON;
            case AVRO:
                return TransformSchemaType.AVRO;
            case PROTOBUF:
                return TransformSchemaType.PROTOBUF;
            default:
                throw new IllegalArgumentException(
                        "Unsupported schema type " + schema.getSchemaInfo().getType());
        }
    }

    @Override
    public void process(MutableRecord mutableRecord) throws Exception {
        TransformFunctionUtil.processTransformSteps(mutableRecord, steps);
    }

    public static Record<GenericObject> send(Context context, MutableRecord mutableRecord)
            throws IOException {
        if (mutableRecord.isDropCurrentRecord()) {
            return null;
        }
        mutableRecord.convertAvroToBytes();
        mutableRecord.convertMapToStringOrBytes();

        Schema outputSchema;
        Object outputObject;
        if (mutableRecord.getKeySchemaType() != null) {
            KeyValueEncodingType keyValueEncodingType =
                    (KeyValueEncodingType)
                            mutableRecord.getCustomContext().get("keyValueEncodingType");
            outputSchema =
                    Schema.KeyValue(
                            buildSchema(
                                    mutableRecord.getKeySchemaType(),
                                    mutableRecord.getKeyNativeSchema()),
                            buildSchema(
                                    mutableRecord.getValueSchemaType(),
                                    mutableRecord.getValueNativeSchema()),
                            keyValueEncodingType != null
                                    ? keyValueEncodingType
                                    : KeyValueEncodingType.INLINE);
            Object outputKeyObject = mutableRecord.getKeyObject();
            Object outputValueObject = mutableRecord.getValueObject();
            outputObject = new KeyValue<>(outputKeyObject, outputValueObject);
        } else {
            outputSchema =
                    buildSchema(
                            mutableRecord.getValueSchemaType(),
                            mutableRecord.getValueNativeSchema());
            outputObject = mutableRecord.getValueObject();
        }

        if (log.isDebugEnabled()) {
            log.debug("output {} schema {}", outputObject, outputSchema);
        }

        FunctionRecord.FunctionRecordBuilder<GenericObject> recordBuilder =
                context.newOutputRecordBuilder(outputSchema)
                        .destinationTopic(mutableRecord.getOutputTopic())
                        .value(outputObject)
                        .properties(mutableRecord.getProperties());

        if (mutableRecord.getKeySchemaType() == null && mutableRecord.getKey() != null) {
            recordBuilder.key(mutableRecord.getKey());
        }

        return recordBuilder.build();
    }

    private static Schema<?> buildSchema(TransformSchemaType schemaType, Object nativeSchema) {
        if (schemaType == null) {
            throw new IllegalArgumentException("Schema type should not be null.");
        }
        switch (schemaType) {
            case INT8:
                return Schema.INT8;
            case INT16:
                return Schema.INT16;
            case INT32:
                return Schema.INT32;
            case INT64:
                return Schema.INT64;
            case FLOAT:
                return Schema.FLOAT;
            case DOUBLE:
                return Schema.DOUBLE;
            case BOOLEAN:
                return Schema.BOOL;
            case STRING:
                return Schema.STRING;
            case BYTES:
                return Schema.BYTES;
            case DATE:
                return Schema.DATE;
            case TIME:
                return Schema.TIME;
            case TIMESTAMP:
                return Schema.TIMESTAMP;
            case INSTANT:
                return Schema.INSTANT;
            case LOCAL_DATE:
                return Schema.LOCAL_DATE;
            case LOCAL_TIME:
                return Schema.LOCAL_TIME;
            case LOCAL_DATE_TIME:
                return Schema.LOCAL_DATE_TIME;
            case AVRO:
                return Schema.NATIVE_AVRO(nativeSchema);
            case JSON:
                return new JsonNodeSchema((org.apache.avro.Schema) nativeSchema);
            default:
                throw new IllegalArgumentException("Unsupported schema type " + schemaType);
        }
    }

    private static String kebabToPascal(String kebab) {
        return Pattern.compile("(?:^|-)(.)")
                .matcher(kebab)
                .replaceAll(mr -> mr.group(1).toUpperCase());
    }

    protected ServiceProvider buildServiceProvider(TransformStepConfig config) {
        if (config != null) {
            if (config.getOpenai() != null) {
                return new OpenAIServiceProvider(config);
            }
            if (config.getHuggingface() != null) {
                return new HuggingFaceProvider()
                        .createImplementation(
                                TransformFunctionUtil.convertToMap(config.getHuggingface()),
                                MetricsReporter.DISABLED);
            }
        }
        return new ServiceProvider.NoopServiceProvider();
    }

    protected QueryStepDataSource buildDataSource(Map<String, Object> dataSourceConfig) {
        return TransformFunctionUtil.buildDataSource(dataSourceConfig);
    }
}
