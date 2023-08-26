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
package ai.langstream.cli.commands;

import ai.langstream.admin.client.AdminClient;
import ai.langstream.admin.client.AdminClientConfiguration;
import ai.langstream.admin.client.AdminClientLogger;
import ai.langstream.cli.LangStreamCLIConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import org.apache.commons.beanutils.PropertyUtils;
import picocli.CommandLine;

public abstract class BaseCmd implements Runnable {

    public enum Formats {
        raw,
        json,
        yaml
    }

    private static final ObjectMapper yamlConfigReader = new ObjectMapper(new YAMLFactory());
    protected static final ObjectMapper jsonPrinter = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
    protected static final ObjectMapper yamlPrinter = new ObjectMapper(new YAMLFactory())
            .enable(SerializationFeature.INDENT_OUTPUT)
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

    @CommandLine.Spec
    protected CommandLine.Model.CommandSpec command;
    private AdminClient client;
    private LangStreamCLIConfig config;


    protected abstract RootCmd getRootCmd();

    protected AdminClient getClient() {
        if (client == null) {
            final AdminClientLogger logger = new AdminClientLogger() {
                @Override
                public void log(Object message) {
                    BaseCmd.this.log(message);
                }

                @Override
                public void error(Object message) {
                    BaseCmd.this.err(message);
                }

                @Override
                public void debug(Object message) {
                    BaseCmd.this.debug(message);
                }
            };
            client = new AdminClient(toAdminConfiguration(getConfig()), logger);
        }
        return client;
    }

    protected LangStreamCLIConfig getConfig() {
        loadConfig();
        return config;
    }

    private static AdminClientConfiguration toAdminConfiguration(LangStreamCLIConfig config) {
        return AdminClientConfiguration.builder()
                .apiGatewayUrl(config.getApiGatewayUrl())
                .webServiceUrl(config.getWebServiceUrl())
                .token(config.getToken())
                .tenant(config.getTenant())
                .build();
    }


    @SneakyThrows
    private void loadConfig() {
        if (config == null) {
            File configFile = computeConfigFile();
            config = yamlConfigReader.readValue(configFile, LangStreamCLIConfig.class);
            overrideFromEnv(config);
        }
    }


    @SneakyThrows
    public void updateConfig(Consumer<LangStreamCLIConfig> consumer) {
        consumer.accept(getConfig());
        File configFile = computeConfigFile();
        Files.write(configFile.toPath(), yamlConfigReader.writeValueAsBytes(config));
    }

    private File computeConfigFile() {
        File configFile;
        final String configPath = getRootCmd().getConfigPath();
        if (configPath == null) {
            String configBaseDir = System.getProperty("basedir");
            if (configBaseDir == null) {
                configBaseDir = System.getProperty("user.dir");
            }
            configFile = Path.of(configBaseDir, "conf", "cli.yaml").toFile();
        } else {
            configFile = new File(configPath);
        }
        return configFile;
    }


    @SneakyThrows
    private void overrideFromEnv(LangStreamCLIConfig config) {
        for (Field field : AdminClientConfiguration.class.getDeclaredFields()) {
            final String name = field.getName();
            final String newValue = System.getenv("LANGSTREAM_" + name);
            if (newValue != null) {
                log("Using env variable: %s=%s".formatted(name, newValue));
                field.trySetAccessible();
                field.set(config, newValue);
            }
        }
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
            .map(String::toUpperCase).toArray();
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
            if (appliedValue instanceof final JsonNode columnValue) {
                if (columnValue.isNull()) {
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
        StringBuilder formatTemplate = new StringBuilder();
        for (int i = 0; i < numColumns; i++) {
            if (i > 0) {
                formatTemplate.append("  ");
            }
            formatTemplate.append("%-15.15s");
        }
        return formatTemplate.toString();
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
