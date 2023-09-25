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
package ai.langstream.agents.vector.milvus;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.milvus.grpc.DataType;
import io.milvus.param.R;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.highlevel.collection.CreateSimpleCollectionParam;
import io.milvus.param.highlevel.dml.SearchSimpleParam;
import io.milvus.param.index.CreateIndexParam;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.IteratorUtils;

@Slf4j
public class MilvusModel {

    private static final ObjectMapper MAPPER = builderMapper();

    private static ObjectMapper builderMapper() {
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(
                DataType.class,
                new StdScalarDeserializer<>(DataType.class) {
                    @Override
                    public DataType deserialize(JsonParser p, DeserializationContext ctxt)
                            throws IOException, JacksonException {
                        String text = p.getText();
                        List<DataType> dataTypes = Arrays.asList(DataType.values());
                        return dataTypes.stream()
                                .filter(s -> s.name().equalsIgnoreCase(text))
                                .findFirst()
                                .orElseThrow(
                                        () ->
                                                new IllegalArgumentException(
                                                        "Cannot find a Milvus datatype "
                                                                + text
                                                                + ", only "
                                                                + dataTypes));
                    }
                });

        module.addDeserializer(
                CreateSimpleCollectionParam.Builder.class,
                new MilvusBuilderDeserializer<>(
                        CreateSimpleCollectionParam.Builder.class,
                        CreateSimpleCollectionParam::newBuilder));

        module.addDeserializer(
                CreateIndexParam.Builder.class,
                new MilvusBuilderDeserializer<>(
                        CreateIndexParam.Builder.class, CreateIndexParam::newBuilder));

        module.addDeserializer(
                CreateCollectionParam.Builder.class,
                new MilvusBuilderDeserializer<>(
                        CreateCollectionParam.Builder.class,
                        CreateCollectionParam::newBuilder,
                        (key, value) -> {
                            log.info("Key: {}, value: {} {}", key, value, value.getClass());
                            switch (key) {
                                case "field-types":
                                    {
                                        if (value instanceof List list) {
                                            List<FieldType> fieldTypes = new ArrayList<>();
                                            for (Object n : list) {
                                                FieldType fieldType =
                                                        mapper.convertValue(n, FieldType.class);
                                                fieldTypes.add(fieldType);
                                            }
                                            return fieldTypes;
                                        } else {
                                            return value;
                                        }
                                    }
                                default:
                                    return value;
                            }
                        }));

        module.addDeserializer(
                SearchParam.Builder.class,
                new MilvusBuilderDeserializer<>(
                        SearchParam.Builder.class, SearchParam::newBuilder));

        module.addDeserializer(
                FieldType.class,
                new MultistepMilvusBuilderDeserializer<>(
                        FieldType.class, FieldType::newBuilder, FieldType.Builder::build));

        module.addDeserializer(
                SearchSimpleParam.Builder.class,
                new MilvusBuilderDeserializer<>(
                        SearchSimpleParam.Builder.class,
                        SearchSimpleParam::newBuilder,
                        (key, value) -> {
                            switch (key) {
                                case "vectors":
                                    {
                                        if (value instanceof List list) {
                                            List<Float> floatList = new ArrayList<>();
                                            for (Object n : list) {
                                                if (n instanceof Number number) {
                                                    floatList.add(number.floatValue());
                                                } else {
                                                    throw new IllegalArgumentException(
                                                            "Value "
                                                                    + n
                                                                    + " is not a number, it is not valid for the vectors field");
                                                }
                                            }
                                            return floatList;
                                        } else {
                                            return value;
                                        }
                                    }
                                default:
                                    return value;
                            }
                        }));
        mapper.registerModule(module);
        return mapper;
    }

    public static ObjectMapper getMapper() {
        return MAPPER;
    }

    public static String convertToJSONName(String camelCase) {
        StringBuilder result = new StringBuilder();

        for (int i = 0; i < camelCase.length(); i++) {
            char currentChar = camelCase.charAt(i);

            // If the current character is uppercase and not the first character, add a hyphen
            if (Character.isUpperCase(currentChar) && i > 0) {
                result.append('-');
            }

            // Convert the current character to lowercase and add it to the result
            result.append(Character.toLowerCase(currentChar));
        }

        return result.toString();
    }

    public static void handleException(R<?> response) throws Exception {
        if (response.getException() != null) {
            throw response.getException();
        }
    }

    private static class MilvusBuilderDeserializer<T> extends StdDeserializer<T> {

        private final Supplier<T> creator;
        private final BiFunction<String, Object, Object> fieldValueConverter;

        public MilvusBuilderDeserializer(Class<T> vc, Supplier<T> creator) {
            super(vc);
            this.creator = creator;
            this.fieldValueConverter = (key, value) -> value;
        }

        public MilvusBuilderDeserializer(
                Class<T> vc,
                Supplier<T> creator,
                BiFunction<String, Object, Object> fieldValueConverter) {
            super(vc);
            this.creator = creator;
            this.fieldValueConverter = fieldValueConverter;
        }

        @Override
        public T deserialize(JsonParser jp, DeserializationContext ctxt)
                throws IOException, JsonProcessingException {
            try {
                JsonNode node = jp.getCodec().readTree(jp);

                T builder = creator.get();

                applyProperties(jp, builder, node, fieldValueConverter);
                return builder;
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

    private static <T> void applyProperties(
            JsonParser jp,
            T builder,
            JsonNode node,
            BiFunction<String, Object, Object> fieldValueConverter)
            throws JsonProcessingException, IllegalAccessException, InvocationTargetException {
        Method[] methods = builder.getClass().getMethods();
        for (Method m : methods) {
            String name = m.getName();
            if (m.getParameterTypes().length != 1) {
                // this doesn't look like a setter
                continue;
            }
            String propertyName;
            if (name.startsWith("with")) {
                propertyName = name.substring(4);
            } else {
                propertyName = name;
            }
            if (log.isDebugEnabled()) {
                log.debug("Method name: {}", m.getName());
            }
            String jsonStilePropertyName = convertToJSONName(propertyName);
            if (log.isDebugEnabled()) {
                log.debug("JSON Property name: {}", jsonStilePropertyName);
            }
            JsonNode jsonNode = node.get(jsonStilePropertyName);
            if (jsonNode != null) {
                Class<?> parameterType = m.getParameterTypes()[0];
                Object value = jp.getCodec().treeToValue(jsonNode, parameterType);
                if (log.isDebugEnabled()) {
                    log.debug("raw value: {}", value.getClass());
                }
                value = fieldValueConverter.apply(jsonStilePropertyName, value);
                if (log.isDebugEnabled()) {
                    log.debug("Applying value: {}", value);
                }
                m.invoke(builder, value);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug(
                            "Property {} not found, only {}",
                            jsonStilePropertyName,
                            IteratorUtils.toList(node.fieldNames()));
                }
            }
        }
    }

    private static class MultistepMilvusBuilderDeserializer<T, R> extends StdDeserializer<T> {

        private final Supplier<R> builderCreator;
        private final Function<R, T> builderCaller;
        private final BiFunction<String, Object, Object> fieldValueConverter;

        public MultistepMilvusBuilderDeserializer(
                Class<T> vc, Supplier<R> builderCreator, Function<R, T> builderCaller) {
            super(vc);
            this.builderCreator = builderCreator;
            this.builderCaller = builderCaller;
            this.fieldValueConverter = (key, value) -> value;
        }

        public MultistepMilvusBuilderDeserializer(
                Class<T> vc,
                Supplier<R> builderCreator,
                Function<R, T> builderCaller,
                BiFunction<String, Object, Object> fieldValueConverter) {
            super(vc);
            this.builderCreator = builderCreator;
            this.builderCaller = builderCaller;
            this.fieldValueConverter = fieldValueConverter;
        }

        @Override
        public T deserialize(JsonParser jp, DeserializationContext ctxt)
                throws IOException, JsonProcessingException {
            try {
                JsonNode node = jp.getCodec().readTree(jp);
                R builder = builderCreator.get();
                applyProperties(jp, builder, node, fieldValueConverter);
                return builderCaller.apply(builder);
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }
}
