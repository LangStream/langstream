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
package ai.langstream.cli.commands;

import ai.langstream.admin.client.AdminClient;
import ai.langstream.admin.client.AdminClientConfiguration;
import ai.langstream.admin.client.AdminClientLogger;
import ai.langstream.cli.LangStreamCLIConfig;
import ai.langstream.cli.NamedProfile;
import ai.langstream.cli.commands.profiles.BaseProfileCmd;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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

    protected static final ObjectMapper yamlConfigReader = new ObjectMapper(new YAMLFactory());
    protected static final ObjectMapper jsonConfigReader = new ObjectMapper();
    protected static final ObjectMapper jsonPrinter =
            new ObjectMapper()
                    .enable(SerializationFeature.INDENT_OUTPUT)
                    .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
    protected static final ObjectMapper yamlPrinter =
            new ObjectMapper(new YAMLFactory())
                    .enable(SerializationFeature.INDENT_OUTPUT)
                    .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

    protected static final ObjectWriter jsonBodyWriter = new ObjectMapper().writer();

    @CommandLine.Spec protected CommandLine.Model.CommandSpec command;
    private AdminClient client;
    private LangStreamCLIConfig config;

    protected abstract RootCmd getRootCmd();

    protected AdminClient getClient() {
        if (client == null) {
            final AdminClientLogger logger =
                    new AdminClientLogger() {
                        @Override
                        public void log(Object message) {
                            BaseCmd.this.log(message);
                        }

                        @Override
                        public void error(Object message) {
                            BaseCmd.this.err(message);
                        }

                        @Override
                        public boolean isDebugEnabled() {
                            return getRootCmd().isVerbose();
                        }

                        @Override
                        public void debug(Object message) {
                            BaseCmd.this.debug(message);
                        }
                    };
            client = new AdminClient(toAdminConfiguration(), logger);
        }
        return client;
    }

    protected LangStreamCLIConfig getConfig() {
        loadConfig();
        return config;
    }

    protected NamedProfile getDefaultProfile() {
        final LangStreamCLIConfig config = getConfig();
        final NamedProfile defaultProfile = new NamedProfile();
        defaultProfile.setName(BaseProfileCmd.DEFAULT_PROFILE_NAME);
        if (config.getWebServiceUrl() == null) {
            defaultProfile.setWebServiceUrl("http://localhost:8090");
        } else {
            defaultProfile.setWebServiceUrl(config.getWebServiceUrl());
        }
        if (config.getApiGatewayUrl() == null) {
            defaultProfile.setApiGatewayUrl("ws://localhost:8091");
        } else {
            defaultProfile.setApiGatewayUrl(config.getApiGatewayUrl());
        }
        if (config.getTenant() == null) {
            defaultProfile.setTenant("default");
        } else {
            defaultProfile.setTenant(config.getTenant());
        }
        defaultProfile.setToken(config.getToken());
        return defaultProfile;
    }

    protected NamedProfile getCurrentProfile() {
        final String profile;
        if (getRootCmd().getProfile() != null) {
            profile = getRootCmd().getProfile();
        } else {
            profile = getConfig().getCurrentProfile();
        }
        if (BaseProfileCmd.DEFAULT_PROFILE_NAME.equals(profile)) {
            return getDefaultProfile();
        }
        final NamedProfile result = getConfig().getProfiles().get(profile);
        if (result == null) {
            throw new IllegalStateException(
                    String.format("No profile '%s' defined in configuration", profile));
        }
        return result;
    }

    private AdminClientConfiguration toAdminConfiguration() {
        final NamedProfile profile = getCurrentProfile();
        if (profile.getWebServiceUrl() == null) {
            throw new IllegalStateException(
                    String.format("No webServiceUrl defined for profile '%s'", profile.getName()));
        }
        return AdminClientConfiguration.builder()
                .webServiceUrl(profile.getWebServiceUrl())
                .token(profile.getToken())
                .tenant(profile.getTenant())
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

    @SneakyThrows
    private File computeRootConfigFile() {
        final String userHome = System.getProperty("user.home");
        if (!userHome.isBlank() && !"?".equals(userHome)) {
            final Path langstreamDir = Path.of(userHome, ".langstream");
            Files.createDirectories(langstreamDir);
            final Path configFile = langstreamDir.resolve("config");
            debug(String.format("Using config file %s", configFile));
            if (!Files.exists(configFile)) {
                debug(String.format("Init config file %s", configFile));
                Files.write(
                        configFile, yamlConfigReader.writeValueAsBytes(new LangStreamCLIConfig()));
            }
            return configFile.toFile();
        }
        String configBaseDir = System.getProperty("basedir");
        if (configBaseDir == null) {
            configBaseDir = System.getProperty("user.dir");
        }
        final Path dir = Path.of(configBaseDir, "conf");
        Files.createDirectories(dir);

        final Path cliYaml = dir.resolve("cli.yaml");
        debug(String.format("Using config file %s", cliYaml));
        if (!Files.exists(cliYaml)) {
            debug(String.format("Init config file %s", cliYaml));
            Files.write(cliYaml, yamlConfigReader.writeValueAsBytes(new LangStreamCLIConfig()));
        }
        return cliYaml.toFile();
    }

    private File computeConfigFile() {
        File configFile;
        final String configPath = getRootCmd().getConfigPath();
        if (configPath == null) {
            configFile = computeRootConfigFile();
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
                log(String.format("Using env variable: %s=%s", name, newValue));
                field.trySetAccessible();
                field.set(config, newValue);
            }
        }
    }

    protected void log(Object log) {
        command.commandLine().getOut().println(log);
    }

    protected void logNoNewline(Object log) {
        command.commandLine().getOut().print(log);
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
    protected void print(
            Formats format,
            Object body,
            String[] columnsForRaw,
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
            default:
                {
                    List<String[]> rows = new ArrayList<>();

                    rows.add(prepareHeaderRow(columnsForRaw));
                    if (readValue.isArray()) {
                        readValue
                                .elements()
                                .forEachRemaining(
                                        element ->
                                                rows.add(
                                                        prepareRawRow(
                                                                element,
                                                                columnsForRaw,
                                                                valueSupplier)));
                    } else {
                        rows.add(prepareRawRow(readValue, columnsForRaw, valueSupplier));
                    }
                    printRows(rows);
                    break;
                }
        }
    }

    private String[] prepareHeaderRow(String[] columnsForRaw) {
        String[] result = new String[columnsForRaw.length];
        for (int i = 0; i < columnsForRaw.length; i++) {
            result[i] = columnsForRaw[i].toUpperCase();
        }
        return result;
    }

    private String[] prepareRawRow(
            JsonNode readValue,
            String[] columnsForRaw,
            BiFunction<JsonNode, String, Object> valueSupplier) {
        final int numColumns = columnsForRaw.length;

        String[] row = new String[numColumns];
        for (int i = 0; i < numColumns; i++) {
            final String column = columnsForRaw[i];
            String strColumn;

            final Object appliedValue = valueSupplier.apply(readValue, column);
            if (appliedValue instanceof JsonNode) {
                final JsonNode columnValue = (JsonNode) appliedValue;
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
        return row;
    }

    private void printRows(List<String[]> rows) {

        int countMax = 0;

        for (String[] row : rows) {
            for (int i = 0; i < row.length; i++) {
                countMax = Math.max(countMax, row[i].length());
            }
        }
        String formatTemplate = computeFormatTemplate(rows.get(0).length, countMax);

        for (String[] row : rows) {
            final String result = String.format(formatTemplate, row);
            log(result);
        }
    }

    private String computeFormatTemplate(int numColumns, int width) {
        StringBuilder formatTemplate = new StringBuilder();
        for (int i = 0; i < numColumns; i++) {
            if (i > 0) {
                formatTemplate.append("  ");
            }
            formatTemplate.append("%-" + width + "." + width + "s");
        }
        return formatTemplate.toString();
    }

    @SneakyThrows
    protected static Object searchValueInJson(Object jsonNode, String path) {
        return searchValueInJson(
                (Map<String, Object>) jsonPrinter.convertValue(jsonNode, Map.class), path);
    }

    @SneakyThrows
    protected static Object searchValueInJson(Map<String, Object> jsonNode, String path) {
        return PropertyUtils.getProperty(jsonNode, path);
    }
}
