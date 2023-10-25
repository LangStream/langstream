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
import ai.langstream.admin.client.HttpRequestFailedException;
import ai.langstream.admin.client.http.HttpClientFacade;
import ai.langstream.cli.CLILogger;
import ai.langstream.cli.LangStreamCLI;
import ai.langstream.cli.LangStreamCLIConfig;
import ai.langstream.cli.NamedProfile;
import ai.langstream.cli.commands.applications.GithubRepositoryDownloader;
import ai.langstream.cli.commands.profiles.BaseProfileCmd;
import ai.langstream.cli.util.git.JGitClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.commons.beanutils.PropertyUtils;
import picocli.CommandLine;

public abstract class BaseCmd implements Runnable {

    public enum Formats {
        raw,
        json,
        yaml,
        mermaid
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

    @AllArgsConstructor
    static final class CLILoggerImpl implements CLILogger {
        private final Supplier<RootCmd> rootCmd;
        protected Supplier<CommandLine.Model.CommandSpec> command;

        @Override
        public void log(Object message) {
            command.get().commandLine().getOut().println(message);
        }

        @Override
        public void error(Object message) {
            if (message == null) {
                return;
            }
            final String error = message.toString();
            if (error.isBlank()) {
                return;
            }
            System.err.println(command.get().commandLine().getColorScheme().errorText(error));
        }

        @Override
        public boolean isDebugEnabled() {
            return rootCmd.get().isVerbose();
        }

        @Override
        public void debug(Object message) {
            if (isDebugEnabled()) {
                log(message);
            }
        }
    }

    @CommandLine.Spec protected CommandLine.Model.CommandSpec command;
    @Getter private final CLILogger logger = new CLILoggerImpl(() -> getRootCmd(), () -> command);
    private final GithubRepositoryDownloader githubRepositoryDownloader =
            new GithubRepositoryDownloader(
                    new JGitClient(), logger, LangStreamCLI.getLangstreamCLIHomeDirectory());
    private AdminClient client;
    private LangStreamCLIConfig config;
    private Map<String, String> applicationDescriptions = new HashMap<>();

    protected abstract RootCmd getRootCmd();

    protected AdminClient getClient() {
        if (client == null) {
            final AdminClientLogger logger = getAdminClientLogger();
            client = new AdminClient(toAdminConfiguration(), logger);
        }
        return client;
    }

    protected AdminClientLogger getAdminClientLogger() {
        return new AdminClientLogger() {
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
        final Path langstreamCLIHomeDirectory = LangStreamCLI.getLangstreamCLIHomeDirectory();
        if (langstreamCLIHomeDirectory != null) {
            final Path configFile = langstreamCLIHomeDirectory.resolve("config");
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
        logger.log(log);
    }

    protected void logNoNewline(Object log) {
        command.commandLine().getOut().print(log);
    }

    protected void err(Object log) {
        logger.error(log);
    }

    protected void debug(Object log) {
        logger.debug(log);
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

    @SneakyThrows
    protected File checkFileExistsOrDownload(String path) {
        if (path == null) {
            return null;
        }
        if (path.startsWith("http://")) {
            throw new IllegalArgumentException("http is not supported. Please use https instead.");
        }
        if (path.startsWith("https://")) {
            return downloadHttpsFile(
                    path,
                    getClient().getHttpClientFacade(),
                    logger,
                    githubRepositoryDownloader,
                    !getRootCmd().isDisableLocalRepositoriesCache());
        }
        if (path.startsWith("file://")) {
            path = path.substring("file://".length());
        }
        final File file = new File(path);
        if (!file.exists()) {
            throw new RuntimeException("File " + path + " does not exist");
        }
        return file;
    }

    public static File downloadHttpsFile(
            String path,
            HttpClientFacade client,
            CLILogger logger,
            GithubRepositoryDownloader githubRepositoryDownloader,
            boolean useLocalGithubRepos)
            throws IOException, HttpRequestFailedException {
        final URI uri = URI.create(path);
        if ("github.com".equals(uri.getHost())) {
            return githubRepositoryDownloader.downloadGithubRepository(uri, useLocalGithubRepos);
        }

        final HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(URI.create(path))
                        .version(HttpClient.Version.HTTP_1_1)
                        .GET()
                        .build();
        final Path tempFile = Files.createTempFile("langstream", ".bin");
        final long start = System.currentTimeMillis();
        // use HttpResponse.BodyHandlers.ofByteArray() to get cleaner error messages
        final HttpResponse<byte[]> response =
                client.http(request, HttpResponse.BodyHandlers.ofByteArray());
        if (response.statusCode() >= 400) {
            throw new RuntimeException(
                    "Failed to download file: "
                            + path
                            + "\nReceived status code: "
                            + response.statusCode()
                            + "\n"
                            + response.body());
        }
        Files.write(tempFile, response.body());
        final long time = (System.currentTimeMillis() - start) / 1000;
        logger.log(String.format("downloaded remote file %s (%d s)", path, time));
        return tempFile.toFile();
    }

    @AllArgsConstructor
    @Getter
    private static class Dependency {
        private String name;
        private String url;
        private String type;
        private String sha512sum;
    }

    public static void downloadDependencies(
            Path directory, AdminClient adminClient, Consumer<String> logger) throws Exception {

        final Path configuration = directory.resolve("configuration.yaml");
        if (!Files.exists(configuration)) {
            return;
        }
        final Map<String, Object> map =
                yamlConfigReader.readValue(configuration.toFile(), Map.class);

        final Map<String, Object> configurationMap = (Map<String, Object>) map.get("configuration");

        final List<Map<String, Object>> dependencies =
                (List<Map<String, Object>>) configurationMap.get("dependencies");
        if (dependencies == null) {
            return;
        }
        final List<Dependency> dependencyList =
                dependencies.stream()
                        .map(
                                dependency ->
                                        new Dependency(
                                                (String) dependency.get("name"),
                                                (String) dependency.get("url"),
                                                (String) dependency.get("type"),
                                                (String) dependency.get("sha512sum")))
                        .collect(Collectors.toList());

        for (Dependency dependency : dependencyList) {
            URL url = new URL(dependency.getUrl());

            final String outputPath;
            if (dependency.getType() == null) {
                throw new RuntimeException("dependency type must be set");
            }
            switch (dependency.getType()) {
                case "java-library":
                    outputPath = "java/lib";
                    break;
                default:
                    throw new RuntimeException(
                            "unsupported dependency type: " + dependency.getType());
            }

            Path output = directory.resolve(outputPath);
            if (!Files.exists(output)) {
                Files.createDirectories(output);
            }
            String rawFileName = url.getFile().substring(url.getFile().lastIndexOf('/') + 1);
            Path fileName = output.resolve(rawFileName);

            if (Files.isRegularFile(fileName)) {

                if (!checkChecksum(fileName, dependency.getSha512sum(), logger)) {
                    logger.accept("File seems corrupted, deleting it");
                    Files.delete(fileName);
                } else {
                    logger.accept(
                            String.format(
                                    "Dependency: %s at %s", fileName, fileName.toAbsolutePath()));
                    continue;
                }
            }

            logger.accept(
                    String.format(
                            "downloading dependency: %s to %s",
                            fileName, fileName.toAbsolutePath()));
            final HttpRequest request = adminClient.newDependencyGet(url);
            adminClient.http(request, HttpResponse.BodyHandlers.ofFile(fileName));

            if (!checkChecksum(fileName, dependency.getSha512sum(), logger)) {
                logger.accept(
                        "File still seems corrupted. Please double check the checksum and try again.");
                Files.delete(fileName);
                throw new IOException("File at " + url + ", seems corrupted");
            }

            logger.accept("dependency downloaded");
        }
    }

    protected static boolean checkChecksum(Path fileName, String sha512sum, Consumer<String> logger)
            throws Exception {
        MessageDigest instance = MessageDigest.getInstance("SHA-512");
        try (DigestInputStream inputStream =
                new DigestInputStream(
                        new BufferedInputStream(Files.newInputStream(fileName)), instance)) {
            while (inputStream.read() != -1) {}
        }
        byte[] digest = instance.digest();
        String base16encoded = bytesToHex(digest);
        if (!sha512sum.equals(base16encoded)) {
            logger.accept(String.format("Computed checksum: %s", base16encoded));
            logger.accept(String.format("Expected checksum: %s", sha512sum));
        }
        return sha512sum.equals(base16encoded);
    }

    private static String bytesToHex(byte[] hash) {
        StringBuilder hexString = new StringBuilder(2 * hash.length);
        for (byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }

    protected String getAppDescriptionOrLoad(String application) {
        return applicationDescriptions.computeIfAbsent(
                application, app -> getClient().applications().get(application, false));
    }

    protected void ensureFormatIn(Formats value, Formats... allowed) {
        final List<Formats> asList = Arrays.stream(allowed).collect(Collectors.toList());
        if (!asList.contains(value)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Format %s is not allowed. Allowed formats are: %s",
                            value,
                            asList.stream().map(Enum::name).collect(Collectors.joining(","))));
        }
    }
}
