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
package ai.langstream.cli.util;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.text.StringSubstitutor;

@Slf4j
public class LocalFileReferenceResolver {

    static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    private static final Pattern filePlaceHolderPattern = Pattern.compile("<file:(.*?)>");

    public static String resolveFileReferencesInYAMLFile(Path file) throws Exception {
        return resolveFileReferencesInYAMLFile(
                Files.readString(file, StandardCharsets.UTF_8),
                filename -> {
                    Path fileToRead = file.getParent().resolve(filename);
                    try {
                        String fileNameString = filename.toString();
                        if (isTextFile(fileNameString)) {
                            log.info("Reading text file {}", fileToRead);
                            return Files.readString(fileToRead, StandardCharsets.UTF_8);
                        } else {
                            byte[] content = Files.readAllBytes(fileToRead);
                            log.info(
                                    "Reading binary file {} and encoding it using base64 encoding",
                                    fileToRead);
                            return "base64:"
                                    + java.util.Base64.getEncoder().encodeToString(content);
                        }
                    } catch (IOException error) {
                        throw new IllegalArgumentException("Cannot read file " + fileToRead, error);
                    }
                });
    }

    private static final Set<String> TEXT_FILES_EXTENSIONS =
            Set.of("txt", "yaml", "yml", "json", "text");

    private static boolean isTextFile(String fileNameString) {
        if (fileNameString == null) {
            return false;
        }
        String lowerCase = fileNameString.toLowerCase();
        for (String ext : TEXT_FILES_EXTENSIONS) {
            if (lowerCase.endsWith(ext)) {
                return true;
            }
        }
        return false;
    }

    public static String resolveFileReferencesInYAMLFile(
            String content, Function<String, String> readFileContents) throws Exception {
        // first of all, we read te file with a generic YAML parser
        // in order to fail fast if the file is not valid YAML
        try {
            Map<String, Object> map = mapper.readValue(content, Map.class);

            if (!filePlaceHolderPattern.matcher(content).find() && !content.contains("${")) {
                return content;
            }

            resolveFileReferencesInMap(map, readFileContents);
            return mapper.writeValueAsString(map);
        } catch (JsonParseException | JsonMappingException error) {
            throw new IllegalArgumentException("Cannot parse YAML file", error);
        }
    }

    private static void resolveFileReferencesInList(
            List<Object> list, Function<String, String> readFileContents) throws Exception {
        for (int i = 0; i < list.size(); i++) {
            Object value = list.get(i);
            if (value instanceof String) {
                String string = (String) value;
                String newValue = resolveReferencesInString(string, readFileContents);
                list.set(i, newValue);
            } else if (value instanceof Map) {
                Map<String, Object> mapChild = (Map<String, Object>) value;
                resolveFileReferencesInMap(mapChild, readFileContents);
            } else if (value instanceof List) {
                List<Object> listChild = (List<Object>) value;
                resolveFileReferencesInList(listChild, readFileContents);
            }
        }
    }

    private static void resolveFileReferencesInMap(
            Map<String, Object> map, Function<String, String> readFileContents) throws Exception {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getValue() instanceof String) {
                String string = (String) entry.getValue();
                String newValue = resolveReferencesInString(string, readFileContents);
                entry.setValue(newValue);
            } else if (entry.getValue() instanceof Map) {
                Map<String, Object> mapChild = (Map<String, Object>) entry.getValue();
                resolveFileReferencesInMap(mapChild, readFileContents);
            } else if (entry.getValue() instanceof List) {
                List<Object> listChild = (List<Object>) entry.getValue();
                resolveFileReferencesInList(listChild, readFileContents);
            }
        }
    }

    /**
     * Resolve references to local files in a text (YAML) file. References are always relative to
     * the directory that contains the file.
     *
     * @param content contents of the YAML file
     * @return the new content of the file
     */
    public static String resolveReferencesInString(
            String content, Function<String, String> readFileContents) throws Exception {

        // first apply the env
        StringSubstitutor stringSubstitutor = new StringSubstitutor(System.getenv());
        content = stringSubstitutor.replace(content);

        // then resolve file references
        Matcher matcher = filePlaceHolderPattern.matcher(content);
        StringBuffer buffer = new StringBuffer();

        while (matcher.find()) {
            String filename = matcher.group(1);
            String replacement = readFileContents.apply(filename);
            matcher.appendReplacement(buffer, replacement);
        }
        matcher.appendTail(buffer);

        return buffer.toString();
    }
}
