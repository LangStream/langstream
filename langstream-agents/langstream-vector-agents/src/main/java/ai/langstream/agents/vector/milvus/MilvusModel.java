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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.highlevel.collection.CreateSimpleCollectionParam;
import io.milvus.param.highlevel.dml.SearchSimpleParam;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MilvusModel {

    private static final ObjectMapper MAPPER = builderMapper();

    private static ObjectMapper builderMapper() {
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(
                CreateSimpleCollectionParam.Builder.class,
                new MilvusBuilderDeserializer<>(
                        CreateSimpleCollectionParam.Builder.class,
                        CreateSimpleCollectionParam::newBuilder));
        module.addDeserializer(
                SearchParam.Builder.class,
                new MilvusBuilderDeserializer<>(
                        SearchParam.Builder.class, SearchParam::newBuilder));
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

                Method[] methods = builder.getClass().getMethods();
                for (Method m : methods) {
                    String name = m.getName();
                    log.info("Method name: {}", m.getName());
                    if (name.startsWith("with")) {
                        String propertyName = name.substring(4);
                        log.info("Property name: {}", propertyName);
                        String jsonStilePropertyName = convertToJSONName(propertyName);
                        log.info("JSON Property name: {}", jsonStilePropertyName);
                        JsonNode jsonNode = node.get(jsonStilePropertyName);
                        if (jsonNode != null) {
                            Object value =
                                    jp.getCodec().treeToValue(jsonNode, m.getParameterTypes()[0]);
                            log.info("raw value: {}", value);
                            value = fieldValueConverter.apply(jsonStilePropertyName, value);
                            log.info("Applying value: {}", value);
                            m.invoke(builder, value);
                        }
                    }
                }
                return builder;
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }
}
