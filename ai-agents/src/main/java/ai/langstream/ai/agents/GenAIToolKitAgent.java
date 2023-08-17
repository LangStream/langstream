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
package ai.langstream.ai.agents;

import ai.langstream.ai.agents.datasource.DataSourceProviderRegistry;
import ai.langstream.ai.agents.services.ServiceProviderRegistry;
import com.datastax.oss.sga.api.runner.code.Header;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.code.SingleRecordAgentProcessor;
import com.datastax.oss.streaming.ai.TransformContext;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import com.datastax.oss.streaming.ai.jstl.predicate.StepPredicatePair;
import com.datastax.oss.streaming.ai.model.TransformSchemaType;
import com.datastax.oss.streaming.ai.model.config.TransformStepConfig;
import com.datastax.oss.streaming.ai.services.ServiceProvider;
import com.datastax.oss.streaming.ai.util.TransformFunctionUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Optional;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;

@Slf4j
public class GenAIToolKitAgent extends SingleRecordAgentProcessor {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private List<StepPredicatePair> steps;
    private TransformStepConfig config;
    private QueryStepDataSource dataSource;
    private ServiceProvider serviceProvider;

    @Override
    public List<Record> processRecord(Record record) throws Exception {
        log.info("Processing {}", record);
        TransformContext context = recordToTransformContext(record);
        if (config.isAttemptJsonConversion()) {
            context.setKeyObject(TransformFunctionUtil.attemptJsonConversion(context.getKeyObject()));
            context.setValueObject(TransformFunctionUtil.attemptJsonConversion(context.getValueObject()));
        }
        // the headers must be Strings, this is a tentative conversion
        // in the future we need a better way to handle headers
        context.setProperties(record
                .headers()
                .stream()
                .filter(h -> h.key() != null && h.value() != null)
                .collect(Collectors.toMap(Header::key, (h -> {
                    if (h.value() == null) {
                        return null;
                    }
                    if (h.value() instanceof byte[]) {
                        return new String((byte[]) h.value(), StandardCharsets.UTF_8);
                    } else {
                        return h.value().toString();
                    }
                }))));
        TransformFunctionUtil.processTransformSteps(context, steps);
        context.convertMapToStringOrBytes();
        Optional<Record> recordResult = transformContextToRecord(context, record.headers());
        return recordResult.isPresent() ? List.of(recordResult.get()) : List.of();
    }

    @Override
    @SneakyThrows
    public void init(Map<String, Object> configuration) {
        configuration = new HashMap<>(configuration);


        // remove this from the config in order to avoid passing it TransformStepConfig
        Map<String, Object> datasourceConfiguration =
                (Map<String, Object>) configuration.remove("datasource");
        serviceProvider = ServiceProviderRegistry.getServiceProvider(configuration);

        configuration.remove("vertex");
        config = MAPPER.convertValue(configuration, TransformStepConfig.class);
        dataSource = DataSourceProviderRegistry.getQueryStepDataSource(datasourceConfiguration);
        steps = TransformFunctionUtil.getTransformSteps(config, serviceProvider, dataSource);
    }

    @Override
    public void close() throws Exception {
        if (dataSource != null) {
            dataSource.close();
        }
        for (StepPredicatePair pair : steps) {
            pair.getTransformStep().close();
        }
        if (serviceProvider != null) {
            serviceProvider.close();
        }
    }

    private TransformContext recordToTransformContext(Record record) {
        TransformContext context = new TransformContext();
        context.setKeyObject(record.key());
        context.setKeySchemaType(record.key() == null ? null : getSchemaType(record.key().getClass()));
        // TODO: temporary hack. We should be able to get the schema from the record
        if (record.key() instanceof GenericRecord) {
            context.setKeyNativeSchema(((GenericRecord) record.key()).getSchema());
        }
        context.setValueObject(record.value());
        context.setValueSchemaType(getSchemaType(record.value().getClass()));
        // TODO: temporary hack. We should be able to get the schema from the record
        if (record.value() instanceof GenericRecord) {
            context.setKeyNativeSchema(((GenericRecord) record.value()).getSchema());
        }
        context.setInputTopic(record.origin());
        context.setEventTime(record.timestamp());
        return context;
    }

    private Optional<Record> transformContextToRecord(TransformContext context, Collection<Header> headers) {
        if (context.isDropCurrentRecord()) {
            return Optional.empty();
        }
        return Optional.of(new TransformRecord(context, headers));
    }

    private record TransformRecord(TransformContext context, Collection<Header> headers) implements Record {
        private TransformRecord(TransformContext context, Collection<Header> headers) {
            this.context = context;
            this.headers = new ArrayList<>(headers);
        }

        @Override
        public Object key() {
            return context.getKeyObject();
        }

        @Override
        public Object value() {
            return context.getValueObject();
        }

        @Override
        public String origin() {
            return context.getInputTopic();
        }

        @Override
        public Long timestamp() {
            return context.getEventTime();
        }
    }

    private static TransformSchemaType getSchemaType(Class<?> javaType) {
        if (String.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.STRING;
        }
        if (Byte.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.INT8;
        }
        if (Short.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.INT16;
        }
        if (Integer.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.INT32;
        }
        if (Long.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.INT64;
        }
        if (Double.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.DOUBLE;
        }
        if (Float.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.FLOAT;
        }
        if (Boolean.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.BOOLEAN;
        }
        if (byte[].class.isAssignableFrom(javaType)) {
            return TransformSchemaType.BYTES;
        }
        // Must be before DATE
        if (Time.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.TIME;
        }
        // Must be before DATE
        if (Timestamp.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.TIMESTAMP;
        }
        if (Date.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.DATE;
        }
        if (Instant.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.INSTANT;
        }
        if (LocalDate.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.LOCAL_DATE;
        }
        if (LocalTime.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.LOCAL_TIME;
        }
        if (LocalDateTime.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.LOCAL_DATE_TIME;
        }
        if (GenericRecord.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.AVRO;
        }
        if (JsonNode.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.JSON;
        }
        throw new IllegalArgumentException("Unsupported data type: " + javaType);
    }
}
