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
package com.datastax.oss.sga.cli.commands;

import com.datastax.oss.sga.cli.SgaCLIConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.ConnectException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.commons.beanutils.PropertyUtils;
import picocli.CommandLine;

public abstract class BaseCmd implements Runnable {

    public enum Formats {
        raw,
        json,
        yaml
    }

    protected static final ObjectMapper yamlConfigReader = new ObjectMapper(new YAMLFactory());
    protected static final ObjectMapper jsonPrinter = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
    protected static final ObjectMapper yamlPrinter = new ObjectMapper(new YAMLFactory())
            .enable(SerializationFeature.INDENT_OUTPUT)
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

    @CommandLine.Spec
    protected CommandLine.Model.CommandSpec command;

    protected abstract RootCmd getRootCmd();

    private HttpClient httpClient;
    private SgaCLIConfig config;


    @SneakyThrows
    protected SgaCLIConfig getConfig() {
        if (config == null) {
            File configFile;
            final RootCmd rootCmd = getRootCmd();
            if (rootCmd.getConfigPath() == null) {
                String configBaseDir = System.getProperty("basedir");
                if (configBaseDir == null) {
                    configBaseDir = System.getProperty("user.dir");
                }
                configFile = Path.of(configBaseDir, "conf", "cli.yaml").toFile();
            } else {
                configFile = new File(rootCmd.getConfigPath());
            }
            if (!configFile.exists()) {
                throw new IllegalStateException("Config file not found: " + configFile);
            }
            config = yamlConfigReader.readValue(configFile, SgaCLIConfig.class);
            overrideFromEnv(config);
        }
        return config;
    }

    @SneakyThrows
    protected void updateConfig(Consumer<SgaCLIConfig> consumer) {
        consumer.accept(getConfig());
        File configFile = computeConfigFile();
        Files.write(configFile.toPath(), yamlConfigReader.writeValueAsBytes(config));
    }

    private File computeConfigFile() {
        File configFile;
        final RootCmd rootCmd = getRootCmd();
        if (rootCmd.getConfigPath() == null) {
            String configBaseDir = System.getProperty("basedir");
            if (configBaseDir == null) {
                configBaseDir = System.getProperty("user.dir");
            }
            configFile = Path.of(configBaseDir, "conf", "cli.yaml").toFile();
        } else {
            configFile = new File(rootCmd.getConfigPath());
        }
        return configFile;
    }


    @SneakyThrows
    private void overrideFromEnv(SgaCLIConfig config) {
        for (Field field : SgaCLIConfig.class.getDeclaredFields()) {
            final String name = field.getName();
            final String newValue = System.getenv("SGA_" + name);
            if (newValue != null) {
                log("Using env variable: %s=%s".formatted(name, newValue));
                field.trySetAccessible();
                field.set(config, newValue);
            }
        }
    }

    protected String getBaseWebServiceUrl() {
        return getConfig().getWebServiceUrl();
    }

    protected synchronized HttpClient getHttpClient() {
        if (httpClient == null) {
            httpClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(30))
                    .followRedirects(HttpClient.Redirect.ALWAYS)
                    .build();
        }
        return httpClient;
    }

    @SneakyThrows
    protected HttpResponse<String> http(HttpRequest httpRequest) {
        return http(httpRequest, HttpResponse.BodyHandlers.ofString());
    }

    protected <T> HttpResponse<T> http(HttpRequest httpRequest, HttpResponse.BodyHandler<T> bodyHandler) {
        try {
            final HttpResponse<T> response = getHttpClient().send(httpRequest, bodyHandler);
            final int status = response.statusCode();
            if (status >= 200 && status < 300) {
                return response;
            }
            if (status >= 400) {
                final T body = response.body();
                if (body != null) {
                    err(body);
                }
                throw new RuntimeException("Request failed: " + response.statusCode());
            }
            throw new RuntimeException("Unexpected status code: " + status);
        } catch (ConnectException error) {
            throw new RuntimeException("Cannot connect to " + getBaseWebServiceUrl(), error);
        } catch (IOException | InterruptedException error) {
            throw new RuntimeException("Unexpected network error " + error, error);
        }
    }

    private HttpRequest.Builder withAuth(HttpRequest.Builder builder) {
        final String token = getConfig().getToken();
        if (token == null) {
            return builder;
        }
        return builder.header("Authorization", "Bearer " + token);
    }

    protected HttpRequest newGet(String uri) {
        return withAuth(
                HttpRequest.newBuilder()
                        .uri(URI.create("%s/api%s".formatted(getBaseWebServiceUrl(), uri)))
                        .version(HttpClient.Version.HTTP_1_1)
                        .GET()
        )
                .build();
    }

    protected HttpRequest newDependencyGet(URL uri) throws URISyntaxException {
        return withAuth(HttpRequest.newBuilder()
                .uri(uri.toURI())
                .version(HttpClient.Version.HTTP_1_1)
                .GET()
        )
                .build();
    }

    protected HttpRequest newDelete(String uri) {
        return withAuth(HttpRequest.newBuilder()
                .uri(URI.create("%s/api%s".formatted(getBaseWebServiceUrl(), uri)))
                .version(HttpClient.Version.HTTP_1_1)
                .DELETE()
        )
                .build();
    }

    protected HttpRequest newPut(String uri, String contentType, HttpRequest.BodyPublisher bodyPublisher) {
        return withAuth(HttpRequest.newBuilder()
                .uri(URI.create("%s/api%s".formatted(getBaseWebServiceUrl(), uri)))
                .header("Content-Type", contentType)
                .version(HttpClient.Version.HTTP_1_1)
                .PUT(bodyPublisher)
        )
                .build();
    }


    protected HttpRequest newPost(String uri, String contentType, HttpRequest.BodyPublisher bodyPublisher) {
        return withAuth(HttpRequest.newBuilder()
                .uri(URI.create("%s/api%s".formatted(getBaseWebServiceUrl(), uri)))
                .header("Content-Type", contentType)
                .version(HttpClient.Version.HTTP_1_1)
                .POST(bodyPublisher)
        )
                .build();
    }


    protected void log(Object log) {
        command.commandLine().getOut().println(log);
    }

    protected void err(Object log) {
        final String error = log.toString();
        if (error.isBlank()) {
            return;
        }
        System.err.println(command.commandLine().getColorScheme().errorText(error));
    }

    protected void debug(Object log) {
        if (getRootCmd().isVerbose()) {
            log(log);
        }
    }

    protected void print(Formats format, Object body, String... columnsForRaw) {
        print(format, body, columnsForRaw, (node, column) -> node.get(column));
    }

    @SneakyThrows
    protected void print(Formats format, Object body, String[] columnsForRaw,
                         BiFunction<JsonNode, String, Object> valueSupplier) {
        if (body == null) {
            return;
        }
        final String stringBody = body.toString();


        final JsonNode readValue = jsonPrinter.readValue(stringBody, JsonNode.class);
        switch (format) {
            case json:
                log(jsonPrinter.writeValueAsString(readValue));
                break;
            case yaml:
                log(yamlPrinter.writeValueAsString(readValue));
                break;
            case raw:
            default: {
                printRawHeader(columnsForRaw);
                if (readValue.isArray()) {
                    readValue.elements()
                            .forEachRemaining(element -> printRawRow(element, columnsForRaw, valueSupplier));
                } else {
                    printRawRow(readValue, columnsForRaw, valueSupplier);
                }
                break;
            }
        }
    }

    private void printRawHeader(String[] columnsForRaw) {
        final String template = computeFormatTemplate(columnsForRaw.length);
        final Object[] columns = Arrays.stream(columnsForRaw)
                .map(String::toUpperCase)
                .collect(Collectors.toList()).stream().toArray();
        final String header = String.format(template, columns);
        log(header);
    }

    private void printRawRow(JsonNode readValue, String[] columnsForRaw,
                             BiFunction<JsonNode, String, Object> valueSupplier) {
        final int numColumns = columnsForRaw.length;
        String formatTemplate = computeFormatTemplate(numColumns);

        String[] row = new String[numColumns];
        for (int i = 0; i < numColumns; i++) {
            final String column = columnsForRaw[i];
            String strColumn;

            final Object appliedValue = valueSupplier.apply(readValue, column);
            if (appliedValue instanceof JsonNode) {
                final JsonNode columnValue = (JsonNode) appliedValue;
                if (columnValue == null || columnValue.isNull()) {
                    strColumn = "";
                } else {
                    strColumn = columnValue.asText();
                }
            } else {
                if (appliedValue == null) {
                    strColumn = "";
                } else {
                    strColumn = appliedValue.toString();
                }
            }
            row[i] = strColumn;
        }
        final String result = String.format(formatTemplate, row);
        log(result);
    }

    private String computeFormatTemplate(int numColumns) {
        String formatTemplate = "";
        for (int i = 0; i < numColumns; i++) {
            if (i > 0) {
                formatTemplate += "  ";
            }
            formatTemplate += "%-15.15s";
        }
        return formatTemplate;
    }


    @SneakyThrows
    protected static Object searchValueInJson(Object jsonNode, String path) {
        return searchValueInJson((Map<String, Object>) jsonPrinter.convertValue(jsonNode, Map.class), path);
    }

    @SneakyThrows
    protected static Object searchValueInJson(Map<String, Object> jsonNode, String path) {
        return PropertyUtils.getProperty(jsonNode, path);
    }

}
