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
package ai.langstream.deployer.k8s.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import lombok.SneakyThrows;

public class SerializationUtil {

    private static final ObjectMapper mapper =
            new ObjectMapper()
                    .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
                    .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

    private static final ObjectMapper jsonPrettyPrint =
            new ObjectMapper()
                    .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
                    .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
                    .configure(SerializationFeature.INDENT_OUTPUT, true)
                    .setSerializationInclusion(JsonInclude.Include.NON_NULL);
    private static final ObjectMapper yamlMapper =
            new ObjectMapper(
                            YAMLFactory.builder()
                                    .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES)
                                    .disable(YAMLGenerator.Feature.SPLIT_LINES)
                                    .build())
                    .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
                    .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    private SerializationUtil() {}

    @SneakyThrows
    public static <T> T deepCloneObject(T object) {
        if (object == null) {
            return null;
        }
        return (T) mapper.readValue(mapper.writeValueAsString(object), object.getClass());
    }

    @SneakyThrows
    public static String writeAsJson(Object object) {
        return mapper.writeValueAsString(object);
    }

    @SneakyThrows
    public static String prettyPrintJson(Object object) {
        return jsonPrettyPrint.writeValueAsString(object);
    }

    @SneakyThrows
    public static <T> T readJson(String string, Class<T> objectClass) {
        return mapper.readValue(string, objectClass);
    }

    @SneakyThrows
    public static <T> T convertValue(Object from, Class<T> objectClass) {
        return mapper.convertValue(from, objectClass);
    }

    @SneakyThrows
    public static byte[] writeAsJsonBytes(Object object) {
        return mapper.writeValueAsBytes(object);
    }

    @SneakyThrows
    public static String writeAsYaml(Object object) {
        return yamlMapper.writeValueAsString(object);
    }

    @SneakyThrows
    public static <T> T readYaml(String yaml, Class<T> toClass) {
        return yamlMapper.readValue(yaml, toClass);
    }

    public static String writeInlineBashJson(Object value) {
        return writeAsJson(value).replace("'", "'\"'\"'");
    }
}
