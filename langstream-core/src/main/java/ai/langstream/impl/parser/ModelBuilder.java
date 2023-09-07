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
package ai.langstream.impl.parser;

import static ai.langstream.api.model.ErrorsSpec.DEAD_LETTER;
import static ai.langstream.api.model.ErrorsSpec.FAIL;
import static ai.langstream.api.model.ErrorsSpec.SKIP;

import ai.langstream.api.model.AgentConfiguration;
import ai.langstream.api.model.Application;
import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.model.ComputeCluster;
import ai.langstream.api.model.Connection;
import ai.langstream.api.model.Dependency;
import ai.langstream.api.model.ErrorsSpec;
import ai.langstream.api.model.Gateway;
import ai.langstream.api.model.Gateways;
import ai.langstream.api.model.Instance;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.Pipeline;
import ai.langstream.api.model.Resource;
import ai.langstream.api.model.ResourcesSpec;
import ai.langstream.api.model.SchemaDefinition;
import ai.langstream.api.model.Secret;
import ai.langstream.api.model.Secrets;
import ai.langstream.api.model.TopicDefinition;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class ModelBuilder {

    static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    @Getter
    public static class ApplicationWithPackageInfo {
        public ApplicationWithPackageInfo(Application application) {
            this.application = application;
        }

        private final Application application;
        private boolean hasInstanceDefinition;
        private boolean hasSecretDefinition;
        private boolean hasAppDefinition;
        private String pyBinariesDigest;
        private String javaBinariesDigest;
    }

    /**
     * Builds an in memory model of the application from the given directories. We read from
     * multiple directories to allow for a modular approach to defining the application. For
     * instance, you can have a directory with the application definition and one directory with the
     * instance.yaml file and the secrets.yaml file. This is server side code and it is expected
     * that the directories are local to the server.
     *
     * @param applicationDirectories a list of directories containing the application definition
     * @return a fully built application instance (application model + instance + secrets)
     * @throws Exception if an error occurs
     */
    public static ApplicationWithPackageInfo buildApplicationInstance(
            List<Path> applicationDirectories, String instanceContent, String secretsContent)
            throws Exception {
        return buildApplicationInstance(
                applicationDirectories,
                instanceContent,
                secretsContent,
                new MessageDigestFunction(DigestUtils::getSha256Digest),
                new MessageDigestFunction(DigestUtils::getSha256Digest));
    }

    static class MessageDigestFunction implements ChecksumFunction {

        private final Supplier<MessageDigest> messageDigestSupplier;
        private MessageDigest messageDigest;

        public MessageDigestFunction(Supplier<MessageDigest> messageDigestSupplier) {
            this.messageDigestSupplier = messageDigestSupplier;
        }

        @Override
        public void appendFile(String filename, byte[] data) {
            if (messageDigest == null) {
                messageDigest = messageDigestSupplier.get();
            }
            messageDigest.update(filename.getBytes(StandardCharsets.UTF_8));
            messageDigest.update(data);
        }

        @Override
        public String digest() {
            if (messageDigest == null) {
                return null;
            }
            return Hex.encodeHexString(messageDigest.digest());
        }
    }

    interface ChecksumFunction {
        void appendFile(String filename, byte[] data);

        String digest();
    }

    static ApplicationWithPackageInfo buildApplicationInstance(
            List<Path> applicationDirectories,
            String instanceContent,
            String secretsContent,
            ChecksumFunction pyChecksumFunction,
            ChecksumFunction javaChecksumFunction)
            throws Exception {
        Map<String, String> applicationContents = new HashMap<>();

        for (Path directory : applicationDirectories) {
            log.info("Parsing directory: {}", directory.toAbsolutePath());
            try (DirectoryStream<Path> paths = Files.newDirectoryStream(directory); ) {
                for (Path path : paths) {
                    if (Files.isRegularFile(path)) {
                        String filename = path.getFileName().toString();
                        if (filename.endsWith(".yaml") || filename.endsWith(".yml")) {
                            try {
                                String text = Files.readString(path, StandardCharsets.UTF_8);
                                final String existingSamePipelineName =
                                        applicationContents.put(
                                                path.getFileName().toString(), text);
                                if (existingSamePipelineName != null) {
                                    throw new IllegalArgumentException(
                                            "Duplicate pipeline file names in the application: "
                                                    + path.getFileName());
                                }
                            } catch (java.nio.charset.MalformedInputException e) {
                                log.warn("Skipping file {} due to encoding error", path);
                            } catch (JsonMappingException e) {
                                log.error("Error parsing file {}", path, e);
                                throw new IllegalArgumentException(
                                        "The file "
                                                + path
                                                + " is not valid YAML ("
                                                + e.getMessage()
                                                + ")",
                                        e);
                            }
                        }
                    } else {
                        if (Files.isDirectory(path)) {
                            String filename = path.getFileName().toString();
                            if (filename.equals("python")) {
                                recursiveAppendFiles(pyChecksumFunction, path, path);
                            } else {
                                if (filename.equals("java")) {
                                    final Path lib = path.resolve("lib");
                                    if (Files.exists(lib) && Files.isDirectory(lib)) {
                                        recursiveAppendFiles(javaChecksumFunction, lib, lib);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        final ApplicationWithPackageInfo applicationWithPackageInfo =
                buildApplicationInstance(applicationContents, instanceContent, secretsContent);

        applicationWithPackageInfo.javaBinariesDigest = javaChecksumFunction.digest();
        applicationWithPackageInfo.pyBinariesDigest = pyChecksumFunction.digest();
        return applicationWithPackageInfo;
    }

    private static void recursiveAppendFiles(
            ChecksumFunction checksumFunction, Path current, Path rootPath) {

        if (Files.isRegularFile(current)) {
            try {
                final Path relativeCurrent = rootPath.relativize(current);
                final byte[] bytes = Files.readAllBytes(current);
                checksumFunction.appendFile(relativeCurrent.toString(), bytes);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        } else {
            final File[] files = current.toFile().listFiles();
            if (files != null) {
                Arrays.sort(files, Comparator.comparing(File::getName));
                for (File file : files) {
                    recursiveAppendFiles(checksumFunction, file.toPath(), rootPath);
                }
            }
        }
    }

    public static ApplicationWithPackageInfo buildApplicationInstance(
            Map<String, String> files, String instanceContent, String secretsContent)
            throws Exception {
        final ApplicationWithPackageInfo applicationWithPackageInfo =
                new ApplicationWithPackageInfo(new Application());
        for (Map.Entry<String, String> entry : files.entrySet()) {
            parseApplicationFile(entry.getKey(), entry.getValue(), applicationWithPackageInfo);
        }
        if (instanceContent != null) {
            applicationWithPackageInfo.hasInstanceDefinition = true;
            parseInstance(instanceContent, applicationWithPackageInfo.getApplication());
        }

        if (secretsContent != null) {
            applicationWithPackageInfo.hasSecretDefinition = true;
            parseSecrets(secretsContent, applicationWithPackageInfo.getApplication());
        }
        return applicationWithPackageInfo;
    }

    private static void parseApplicationFile(
            String fileName, String content, ApplicationWithPackageInfo applicationWithPackageInfo)
            throws IOException {
        if (!isPipelineFile(fileName)) {
            // skip
            log.info("Skipping {}", fileName);
            return;
        }

        switch (fileName) {
            case "instance.yaml":
                throw new IllegalArgumentException(
                        "instance.yaml must not be included in the application zip");
            case "secrets.yaml":
                throw new IllegalArgumentException(
                        "secrets.yaml must not be included in the application zip");
            case "configuration.yaml":
                applicationWithPackageInfo.hasAppDefinition = true;
                parseConfiguration(content, applicationWithPackageInfo.getApplication());
                break;
            case "gateways.yaml":
                applicationWithPackageInfo.hasAppDefinition = true;
                parseGateways(content, applicationWithPackageInfo.getApplication());
                break;
            default:
                applicationWithPackageInfo.hasAppDefinition = true;
                parsePipelineFile(fileName, content, applicationWithPackageInfo.getApplication());
                break;
        }
    }

    private static boolean isPipelineFile(String fileName) {
        return fileName.endsWith(".yaml");
    }

    private static void parseConfiguration(String content, Application application)
            throws IOException {
        ConfigurationFileModel configurationFileModel =
                mapper.readValue(content, ConfigurationFileModel.class);
        ConfigurationNodeModel configurationNode = configurationFileModel.configuration();
        if (configurationNode != null && configurationNode.resources != null) {
            configurationNode.resources.forEach(
                    r -> {
                        String id = r.id();
                        if (id == null) {
                            id = r.name();
                        }
                        if (id == null) {
                            throw new RuntimeException("Resource 'name' is required");
                        }
                        application.getResources().put(id, r);
                    });
        }
        if (configurationNode != null && configurationNode.dependencies != null) {
            List<Dependency> dependencies = configurationNode.dependencies;
            if (dependencies != null) {
                application.setDependencies(dependencies);
                log.info("Dependencies: {}", application.getDependencies());
            }
        }
        log.info("Configuration: {}", configurationFileModel);
    }

    private static void parseGateways(String content, Application application) throws IOException {
        Gateways gatewaysFileModel = mapper.readValue(content, Gateways.class);
        if (gatewaysFileModel.gateways() != null) {
            gatewaysFileModel.gateways().forEach(ModelBuilder::validateGateway);
        }
        log.info("Gateways: {}", gatewaysFileModel);
        application.setGateways(gatewaysFileModel);
    }

    private static void validateGateway(Gateway gateway) {
        if (gateway.id() == null || gateway.id().isBlank()) {
            throw new IllegalArgumentException("Gateway id is required");
        }
        if (gateway.type() == null) {
            throw new IllegalArgumentException("Gateway type is required");
        }
        if (gateway.type() == Gateway.GatewayType.consume) {
            if (gateway.produceOptions() != null) {
                throw new IllegalArgumentException(
                        "Gateway of type 'consume' cannot have produce options");
            }
            if (gateway.consumeOptions() != null) {
                if (gateway.consumeOptions().filters() != null) {
                    final Gateway.ConsumeOptionsFilters filters =
                            gateway.consumeOptions().filters();
                    if (filters.headers() != null) {
                        filters.headers().forEach(ModelBuilder::validateGatewayKeyValueComparison);
                    }
                }
            }
        } else if (gateway.type() == Gateway.GatewayType.produce) {
            if (gateway.consumeOptions() != null) {
                throw new IllegalArgumentException(
                        "Gateway of type 'produce' cannot have consume options");
            }
        }
    }

    private static void validateGatewayKeyValueComparison(
            Gateway.KeyValueComparison keyValueComparison) {
        if (keyValueComparison.key() == null || keyValueComparison.key().isBlank()) {
            throw new IllegalArgumentException("'key' is required for filter");
        }
        if (keyValueComparison.value() == null
                && keyValueComparison.valueFromParameters() == null
                && keyValueComparison.valueFromAuthentication() == null) {
            throw new IllegalArgumentException(
                    "One of 'value', 'valueFromParameters' or 'valueFromAuthentication' must be specified for filter");
        }
        if (keyValueComparison.value() != null
                && keyValueComparison.valueFromParameters() != null) {
            throw new IllegalArgumentException(
                    "Only one of 'value', 'valueFromParameters' or 'valueFromAuthentication' can be specified for filter");
        }
        if (keyValueComparison.value() != null
                && keyValueComparison.valueFromAuthentication() != null) {
            throw new IllegalArgumentException(
                    "Only one of 'value', 'valueFromParameters' or 'valueFromAuthentication' can be specified for filter");
        }
        if (keyValueComparison.value() != null && keyValueComparison.value().isBlank()) {
            throw new IllegalArgumentException("'value' cannot be blank for filter");
        }
        if (keyValueComparison.valueFromParameters() != null
                && keyValueComparison.valueFromParameters().isBlank()) {
            throw new IllegalArgumentException("'valueFromParameters' cannot be blank for filter");
        }
        if (keyValueComparison.valueFromAuthentication() != null
                && keyValueComparison.valueFromAuthentication().isBlank()) {
            throw new IllegalArgumentException(
                    "'valueFromAuthentication' cannot be blank for filter");
        }
    }

    private static void parsePipelineFile(String filename, String content, Application application)
            throws IOException {
        PipelineFileModel pipelineConfiguration =
                mapper.readValue(content, PipelineFileModel.class);
        if (pipelineConfiguration.getModule() == null) {
            pipelineConfiguration.setModule(Module.DEFAULT_MODULE);
        }
        Module module = application.getModule(pipelineConfiguration.getModule());
        log.info("Configuration: {}", pipelineConfiguration);
        String id = pipelineConfiguration.getId();
        if (id == null) {
            id = filename.replace(".yaml", "").replace(".yml", "");
        }
        Pipeline pipeline = module.addPipeline(id);
        pipeline.setName(pipelineConfiguration.getName());
        pipeline.setResources(
                pipelineConfiguration.getResources() != null
                        ? pipelineConfiguration
                                .getResources()
                                .withDefaultsFrom(ResourcesSpec.DEFAULT)
                        : ResourcesSpec.DEFAULT);
        pipeline.setErrors(
                pipelineConfiguration.getErrors() != null
                        ? pipelineConfiguration.getErrors().withDefaultsFrom(ErrorsSpec.DEFAULT)
                        : ErrorsSpec.DEFAULT);
        validateErrorsSpec(pipeline.getErrors());
        AgentConfiguration last = null;

        if (pipelineConfiguration.getTopics() != null) {
            for (TopicDefinitionModel topicDefinition : pipelineConfiguration.getTopics()) {
                module.addTopic(
                        new TopicDefinition(
                                topicDefinition.getName(),
                                topicDefinition.getCreationMode(),
                                false,
                                topicDefinition.getPartitions(),
                                topicDefinition.getKeySchema(),
                                topicDefinition.getSchema(),
                                topicDefinition.getOptions(),
                                topicDefinition.getConfig()));
            }
        }

        if (pipelineConfiguration.getAssets() != null) {
            for (AssetDefinitionModel assetDefinition : pipelineConfiguration.getAssets()) {
                String assetId =
                        Objects.requireNonNullElse(
                                assetDefinition.getId(), assetDefinition.getName());
                if (assetId == null || assetId.isEmpty()) {
                    throw new IllegalArgumentException("Asset id or name are required");
                }
                String name = assetDefinition.getName();
                if (assetDefinition.getName() == null) {
                    name = assetDefinition.getId();
                }
                module.addAsset(
                        new AssetDefinition(
                                assetId,
                                name,
                                assetDefinition.getCreationMode(),
                                assetDefinition.getAssetType(),
                                assetDefinition.getConfig()));
            }
        }

        int autoId = 1;
        if (pipelineConfiguration.getPipeline() != null) {
            for (AgentModel agent : pipelineConfiguration.getPipeline()) {
                AgentConfiguration agentConfiguration = agent.toAgentConfiguration(pipeline);
                if (agentConfiguration.getType() == null
                        || agentConfiguration.getType().isBlank()) {
                    if (agentConfiguration.getId() != null) {
                        throw new IllegalArgumentException(
                                "Agent type is always required (check agent id "
                                        + agentConfiguration.getId()
                                        + ")");
                    } else if (agentConfiguration.getName() != null) {
                        throw new IllegalArgumentException(
                                "Agent type is always required (check agent name "
                                        + agentConfiguration.getName()
                                        + ")");
                    } else {
                        throw new IllegalArgumentException(
                                "Agent type is always required (there is an agent without type, id or name)");
                    }
                }
                ErrorsSpec errorsSpec = validateErrorsSpec(agentConfiguration.getErrors());
                if (agentConfiguration.getId() == null) {
                    // ensure that we always have an id
                    // please note that this algorithm should not be changed in order to not break
                    // compatibility with existing pipelineConfiguration files
                    String moduleAutoId;
                    if (Objects.equals(Module.DEFAULT_MODULE, module.getId())) {
                        moduleAutoId = "";
                    } else {
                        moduleAutoId = module.getId() + "-";
                    }
                    String autoIdStr =
                            moduleAutoId
                                    + pipeline.getId()
                                    + "-"
                                    + agentConfiguration.getType()
                                    + "-"
                                    + autoId;
                    agentConfiguration.setId(autoIdStr);
                    autoId++;
                }

                if (agent.getInput() != null) {
                    agentConfiguration.setInput(
                            Connection.fromTopic(module.resolveTopic(agent.getInput())));
                }
                if (agent.getOutput() != null) {
                    agentConfiguration.setOutput(
                            Connection.fromTopic(module.resolveTopic(agent.getOutput())));
                }
                Connection input = agentConfiguration.getInput();
                if (last != null && input == null) {
                    // assume that the previous agent is the output of this one
                    agentConfiguration.setInput(Connection.fromAgent(last));

                    // if the previous agent does not have an output, bind to this agent
                    if (last.getOutput() == null) {
                        last.setOutput(Connection.fromAgent(agentConfiguration));

                        ErrorsSpec otherAgentErrorSpecs = agentConfiguration.getErrors();
                        if (Objects.equals(otherAgentErrorSpecs.getOnFailure(), DEAD_LETTER)) {
                            last.setOutput(last.getOutput().withDeadletter(true));
                        }
                    }
                }

                // activate deadletter on the input connection if the agent has deadletter enabled
                if (Objects.equals(errorsSpec.getOnFailure(), DEAD_LETTER)
                        && agentConfiguration.getInput() != null) {
                    agentConfiguration.setInput(agentConfiguration.getInput().withDeadletter(true));
                }

                pipeline.addAgentConfiguration(agentConfiguration);
                last = agentConfiguration;
            }
        }
    }

    private static void parseSecrets(String content, Application application) throws IOException {
        SecretsFileModel secretsFileModel = mapper.readValue(content, SecretsFileModel.class);
        if (log.isDebugEnabled()) { // don't write secrets in logs
            log.debug("Secrets: {}", secretsFileModel);
        }
        final Set<String> ids = new HashSet<>();
        secretsFileModel.secrets.stream()
                .forEach(
                        s -> {
                            validateSecret(s, ids);
                        });
        application.setSecrets(
                new Secrets(
                        secretsFileModel.secrets().stream()
                                .collect(Collectors.toMap(Secret::id, Function.identity()))));
    }

    private static void validateSecret(Secret s, Set<String> ids) {
        if (StringUtils.isBlank(s.id())) {
            throw new IllegalArgumentException("Found secret without id: " + s);
        }
        if (!ids.add(s.id())) {
            throw new IllegalArgumentException("Found duplicate secret id: " + s.id());
        }
    }

    private static void parseInstance(String content, Application application) throws IOException {
        InstanceFileModel instanceModel = mapper.readValue(content, InstanceFileModel.class);
        log.info("Instance Configuration: {}", instanceModel);
        Instance instance = instanceModel.instance;

        // add default "kubernetes" compute cluster if not present
        if (instance.computeCluster() == null) {
            instance =
                    new Instance(
                            instance.streamingCluster(),
                            new ComputeCluster("kubernetes", Map.of()),
                            instance.globals());
        }
        application.setInstance(instance);
    }

    @Data
    public static final class PipelineFileModel {
        private String module = Module.DEFAULT_MODULE;
        private String id;
        private String name;
        private List<AgentModel> pipeline = new ArrayList<>();

        private List<TopicDefinitionModel> topics = new ArrayList<>();

        private List<AssetDefinitionModel> assets = new ArrayList<>();

        private ResourcesSpec resources;
        private ErrorsSpec errors;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class TopicDefinitionModel {
        private String name;

        @JsonProperty("creation-mode")
        private String creationMode;

        private SchemaDefinition schema;

        private int partitions = 0;

        private SchemaDefinition keySchema;
        private Map<String, Object> options;
        private Map<String, Object> config;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class AssetDefinitionModel {
        private String id;
        private String name;

        @JsonProperty("creation-mode")
        private String creationMode;

        @JsonProperty("asset-type")
        private String assetType;

        private Map<String, Object> config;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class AgentModel {
        private String id;
        private String name;
        private String type;
        private String input;
        private String output;
        private Map<String, Object> configuration = new HashMap<>();

        private ResourcesSpec resources;
        private ErrorsSpec errors;

        AgentConfiguration toAgentConfiguration(Pipeline pipeline) {
            AgentConfiguration res = new AgentConfiguration();
            res.setId(id);
            res.setName(name);
            res.setType(type);
            res.setConfiguration(configuration);
            res.setResources(
                    resources == null
                            ? pipeline.getResources()
                            : resources.withDefaultsFrom(pipeline.getResources()));
            res.setErrors(
                    errors == null
                            ? pipeline.getErrors()
                            : errors.withDefaultsFrom(pipeline.getErrors()));
            return res;
        }
    }

    public record ConfigurationNodeModel(List<Resource> resources, List<Dependency> dependencies) {}

    public record ConfigurationFileModel(ConfigurationNodeModel configuration) {}

    public record SecretsFileModel(List<Secret> secrets) {}

    public record InstanceFileModel(Instance instance) {}

    static ErrorsSpec validateErrorsSpec(ErrorsSpec errorsSpec) {
        if (errorsSpec.getRetries() != null && errorsSpec.getRetries() < 0) {
            throw new IllegalArgumentException(
                    "retries must be a positive integer (bad value retries: "
                            + errorsSpec.getRetries()
                            + ")");
        }
        if (errorsSpec.getOnFailure() != null) {
            switch (errorsSpec.getOnFailure()) {
                case ErrorsSpec.SKIP:
                case FAIL:
                case ErrorsSpec.DEAD_LETTER:
                    break;
                default:
                    throw new IllegalArgumentException(
                            "on-failure must be one of '"
                                    + FAIL
                                    + "',  '"
                                    + DEAD_LETTER
                                    + "' or '"
                                    + SKIP
                                    + "' (bad value on-failure: "
                                    + errorsSpec.getOnFailure()
                                    + ")");
            }
        }
        return errorsSpec;
    }
}
