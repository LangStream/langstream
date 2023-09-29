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
package ai.langstream.runtime.doc;

import ai.langstream.api.runner.code.AgentCodeRegistry;
import ai.langstream.api.runner.code.doc.AgentConfiguration;
import ai.langstream.api.runner.code.doc.AgentConfigurationModel;
import ai.langstream.api.runner.code.doc.ConfigProperty;
import ai.langstream.api.runner.code.doc.ConfigPropertyIgnore;
import ai.langstream.impl.nar.NarFileHandler;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.victools.jsonschema.generator.ConfigFunction;
import com.github.victools.jsonschema.generator.FieldScope;
import com.github.victools.jsonschema.generator.MemberScope;
import com.github.victools.jsonschema.generator.Option;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaGenerator;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfig;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder;
import com.github.victools.jsonschema.generator.SchemaVersion;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;
import java.io.File;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

@Slf4j
public class AgentDocumentationGenerator {

    static final ObjectMapper jsonWriter =
            new ObjectMapper()
                    .configure(SerializationFeature.INDENT_OUTPUT, true)
                    .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    static final String USAGE = "Usage: agent-doc-generator <output-file> <version> <agents-directory>";

    @SneakyThrows
    public static void main(String[] args) {
        try {
            if (args.length < 3) {
                System.out.println(USAGE);
                System.exit(-1);
            }
            final String outputFile = args[0];
            final String version = args[1];
            final String agentsDirectory = args[2];

            generate(outputFile, version, agentsDirectory);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static void generate(String outputFile, String version, String agentsDirectory) throws Exception {
        final AgentsConfigurationModel model = generateModel(version, agentsDirectory);
        jsonWriter.writeValue(new File(outputFile), model);
        log.info("Generated documentation for {} agents to {}", model.agents().size(), outputFile);
    }

    @Data
    public static class ConfigPropertyModel {
        private boolean ignore;
        private boolean required;
        private String description;
        private String defaultValue;
    }

    @Data
    public static class ParsedJsonSchema {

        @Data
        public static class Prop {
            String type;
            String description;
            Map<String, Prop> properties;
            Prop items;
        }

        Map<String, Prop> properties;
    }

    public static AgentConfigurationModel readFromClass(Class clazz) {

        final AgentConfiguration agentConfig =
                (AgentConfiguration) clazz.getAnnotation(AgentConfiguration.class);
        if (agentConfig == null) {
            return null;
        }

        AgentConfigurationModel model = new AgentConfigurationModel();
        if (agentConfig.description() != null && !agentConfig.description().isBlank()) {
            model.setDescription(agentConfig.description().strip());
        }
        if (agentConfig.name() != null && !agentConfig.name().isBlank()) {
            model.setName(agentConfig.name());
        }
        model.setProperties(readPropertiesFromClass(clazz));
        return model;
    }


    public static Map<String, AgentConfigurationModel.AgentConfigurationProperty>
            readPropertiesFromClass(Class clazz) {
        JsonNode jsonSchema = getJsonSchema(clazz);
        final ObjectMapper mapper =
                new ObjectMapper()
                        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        final ParsedJsonSchema parsed = mapper.convertValue(jsonSchema, ParsedJsonSchema.class);

        Map<String, AgentConfigurationModel.AgentConfigurationProperty> props =
                new LinkedHashMap<>();
        for (Map.Entry<String, ParsedJsonSchema.Prop> schema : parsed.getProperties().entrySet()) {
            final AgentConfigurationModel.AgentConfigurationProperty parsedProp =
                    parseProp(mapper, schema.getValue());
            if (parsedProp != null) {
                props.put(schema.getKey(), parsedProp);
            }
        }
        return props;
    }

    @SneakyThrows
    private static AgentConfigurationModel.AgentConfigurationProperty parseProp(
            ObjectMapper mapper, ParsedJsonSchema.Prop value) {
        AgentConfigurationModel.AgentConfigurationProperty newProp =
                new AgentConfigurationModel.AgentConfigurationProperty();

        newProp.setType(value.getType());
        final String jsonDesc = value.getDescription();
        if (jsonDesc != null) {
            final ConfigPropertyModel property =
                    mapper.readValue(jsonDesc, ConfigPropertyModel.class);
            if (property.isIgnore()) {
                return null;
            }
            newProp.setRequired(property.isRequired());
            newProp.setDescription(property.getDescription().strip());
            newProp.setDefaultValue(property.getDefaultValue());
        }

        if (value.getProperties() != null) {
            newProp.setProperties(
                    value.getProperties().entrySet().stream()
                            .map(prop -> Pair.of(prop.getKey(), parseProp(mapper, prop.getValue())))
                            .filter(prop -> prop.getRight() != null)
                            .collect(Collectors.toMap(Pair::getLeft, Pair::getRight)));
        }
        if (value.getItems() != null) {
            final AgentConfigurationModel.AgentConfigurationProperty items =
                    parseProp(mapper, value.getItems());

            if (items != null) {
                newProp.setItems(items);
            }
        }
        return newProp;
    }

    private static JsonNode getJsonSchema(Class clazz) {
        SchemaGeneratorConfigBuilder configBuilder =
                new SchemaGeneratorConfigBuilder(
                        SchemaVersion.DRAFT_2020_12, OptionPreset.PLAIN_JSON);
        configBuilder.with(Option.DEFINITIONS_FOR_ALL_OBJECTS).with(Option.INLINE_ALL_SCHEMAS);
        configBuilder
                .forFields()
                // support for @JsonProperty
                .withPropertyNameOverrideResolver(
                        AgentDocumentationGenerator
                                ::getPropertyNameOverrideBasedOnJsonPropertyAnnotation)
                .withDescriptionResolver(
                        new ConfigFunction<FieldScope, String>() {
                            @Override
                            @SneakyThrows
                            public String apply(FieldScope fieldScope) {
                                final ConfigPropertyModel model = new ConfigPropertyModel();
                                boolean isIgnore =
                                        fieldScope.getAnnotation(ConfigPropertyIgnore.class)
                                                != null;
                                model.setIgnore(isIgnore);
                                if (!isIgnore) {
                                    final ConfigProperty annotation =
                                            fieldScope.getAnnotation(ConfigProperty.class);
                                    if (annotation != null) {
                                        model.setRequired(annotation.required());
                                        if (annotation.description() != null
                                                && !annotation.description().isEmpty()) {
                                            model.setDescription(annotation.description());
                                        }
                                        if (annotation.defaultValue() != null
                                                && !annotation.defaultValue().isEmpty()) {
                                            model.setDefaultValue(annotation.defaultValue());
                                        }
                                    }
                                }

                                return jsonWriter.writeValueAsString(model);
                            }
                        });
        SchemaGeneratorConfig config = configBuilder.build();
        SchemaGenerator generator = new SchemaGenerator(config);
        JsonNode jsonSchema = generator.generateSchema(clazz);
        return jsonSchema;
    }

    private static String getPropertyNameOverrideBasedOnJsonPropertyAnnotation(
            MemberScope<?, ?> member) {
        JsonProperty annotation =
                (JsonProperty) member.getAnnotationConsideringFieldAndGetter(JsonProperty.class);
        if (annotation != null) {
            String nameOverride = annotation.value();
            if (nameOverride != null
                    && !nameOverride.isEmpty()
                    && !nameOverride.equals(member.getDeclaredName())) {
                return nameOverride;
            }
        }

        return null;
    }

    private static AgentsConfigurationModel generateModel(String version, String agentsDirectory) throws Exception {

        Map<String, AgentConfigurationModel> agents = new TreeMap<>();
        NarFileHandler narFileHandler =
                new NarFileHandler(
                        Path.of(agentsDirectory),
                        List.of(),
                        Thread.currentThread().getContextClassLoader());
        narFileHandler.scan();

        AgentCodeRegistry agentCodeRegistry = new AgentCodeRegistry();
        agentCodeRegistry.setAgentPackageLoader(narFileHandler);
        agentCodeRegistry.forEachSystemAgent(
                (type, agent) -> {
                    if (agents.containsKey(type)) {
                        return;
                    }
                    log.info("Scanning agent: {}", type);
                    agent.executeNoExceptionWithContextClassloader(
                            ignore -> {
                                try (ScanResult scanResult =
                                        new ClassGraph()
                                                .enableClassInfo()
                                                .enableAnnotationInfo()
                                                .ignoreParentClassLoaders()
                                                .scan()) {
                                    final List<ClassInfo> classInfos =
                                            scanResult
                                                    .getClassesWithAnnotation(
                                                            AgentConfiguration.class)
                                                    .stream()
                                                    .toList();
                                    if (classInfos.isEmpty()) {
                                        log.info("No configuration class found for agent: {}", type);
                                        agents.put(type, new AgentConfigurationModel());
                                    } else {
                                        if (classInfos.size() > 1) {
                                            log.warn(
                                                    "Found more than one configuration class for agent: {}",
                                                    type);
                                        }
                                        final ClassInfo classInfo = classInfos.get(0);
                                        log.info("Scanning configuration class: {}", classInfo.getName());
                                        agents.put(type, readFromClass(classInfo.loadClass()));
                                    }
                                }
                            });
                });
        return new AgentsConfigurationModel(version, agents);
    }
}
