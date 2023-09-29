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
package ai.langstream.impl.uti;

import ai.langstream.api.doc.AgentConfig;
import ai.langstream.api.doc.AgentConfigurationModel;
import ai.langstream.api.doc.ConfigProperty;
import ai.langstream.api.doc.ConfigPropertyIgnore;
import ai.langstream.api.model.AgentConfiguration;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.github.victools.jsonschema.generator.ConfigFunction;
import com.github.victools.jsonschema.generator.FieldScope;
import com.github.victools.jsonschema.generator.MemberScope;
import com.github.victools.jsonschema.generator.Option;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaGenerator;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfig;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder;
import com.github.victools.jsonschema.generator.SchemaVersion;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;

public class ClassConfigValidator {

    static final ObjectMapper jsonWriter =
            new ObjectMapper()
                    .configure(SerializationFeature.INDENT_OUTPUT, true)
                    .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    static final ObjectMapper validatorMapper =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    static final Map<String, AgentConfigurationModel> agentModels = new ConcurrentHashMap<>();

    public static AgentConfigurationModel generateAgentModelFromClass(Class clazz) {
        return agentModels.computeIfAbsent(
                clazz.getName(), k -> generateModelFromClassNoCache(clazz));
    }

    private static AgentConfigurationModel generateModelFromClassNoCache(Class clazz) {
        AgentConfigurationModel model = new AgentConfigurationModel();

        final AgentConfig agentConfig = (AgentConfig) clazz.getAnnotation(AgentConfig.class);
        if (agentConfig != null) {
            if (agentConfig.description() != null && !agentConfig.description().isBlank()) {
                model.setDescription(agentConfig.description().strip());
            }
            if (agentConfig.name() != null && !agentConfig.name().isBlank()) {
                model.setName(agentConfig.name());
            }
        }
        model.setProperties(readPropertiesFromClass(clazz));
        return model;
    }

    @SneakyThrows
    public static void validateAgentModelFromClass(
            AgentConfiguration agentConfiguration, Class modelClazz) {
        validateAgentModelFromClass(
                agentConfiguration, modelClazz, agentConfiguration.getConfiguration());
    }

    @SneakyThrows
    public static void validateAgentModelFromClass(
            AgentConfiguration agentConfiguration, Class modelClazz, Map<String, Object> asMap) {
        asMap = validatorMapper.readValue(validatorMapper.writeValueAsBytes(asMap), Map.class);

        final AgentConfigurationModel agentConfigurationModel =
                generateAgentModelFromClass(modelClazz);

        validateProperties(
                agentConfiguration, null, asMap, agentConfigurationModel.getProperties());

        try {
            validatorMapper.convertValue(asMap, modelClazz);
        } catch (IllegalArgumentException ex) {
            if (ex.getCause() instanceof MismatchedInputException mismatchedInputException) {
                final String property =
                        mismatchedInputException.getPath().stream()
                                .map(r -> r.getFieldName())
                                .collect(Collectors.joining("."));
                throw new IllegalArgumentException(
                        formatErrString(
                                agentConfiguration,
                                property,
                                "has a wrong data type. Expected type: "
                                        + mismatchedInputException.getTargetType().getName()));
            } else {
                throw ex;
            }
        }
    }

    private static String formatErrString(
            AgentConfiguration agent, String property, String message) {
        return "Found error on an agent configuration (agent: '%s', type: '%s'). Property '%s' %s"
                .formatted(
                        agent.getName() == null ? agent.getId() : agent.getName(),
                        agent.getType(),
                        property,
                        message);
    }

    private static void validateProperties(
            AgentConfiguration agentConfiguration,
            String parentProp,
            Map<String, Object> asMap,
            Map<String, AgentConfigurationModel.AgentConfigurationProperty> properties) {
        if (asMap != null) {
            for (String key : asMap.keySet()) {
                if (!properties.containsKey(key)) {
                    final String fullPropertyKey =
                            parentProp == null ? key : parentProp + "." + key;
                    throw new IllegalArgumentException(
                            formatErrString(agentConfiguration, fullPropertyKey, "is unknown"));
                }
            }
        }

        for (Map.Entry<String, AgentConfigurationModel.AgentConfigurationProperty> property :
                properties.entrySet()) {
            final AgentConfigurationModel.AgentConfigurationProperty propertyValue =
                    property.getValue();
            final String propertyKey = property.getKey();
            validateProperty(
                    agentConfiguration,
                    parentProp,
                    asMap == null ? null : asMap.get(propertyKey),
                    propertyValue,
                    propertyKey);
        }
    }

    private static void validateProperty(
            AgentConfiguration agentConfiguration,
            String parentProp,
            Object actualValue,
            AgentConfigurationModel.AgentConfigurationProperty propertyValue,
            String propertyKey) {

        final String fullPropertyKey =
                parentProp == null ? propertyKey : parentProp + "." + propertyKey;

        if (propertyValue.isRequired()) {
            if (actualValue == null) {
                throw new IllegalArgumentException(
                        formatErrString(agentConfiguration, fullPropertyKey, "is required"));
            }
        }
        if (propertyValue.getProperties() != null) {
            validateProperties(
                    agentConfiguration,
                    fullPropertyKey,
                    actualValue == null ? null : (Map<String, Object>) actualValue,
                    propertyValue.getProperties());
        }
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

    public static Map<String, AgentConfigurationModel.AgentConfigurationProperty>
            readPropertiesFromClass(Class clazz) {
        JsonNode jsonSchema = getJsonSchema(clazz);
        final ObjectMapper mapper =
                new ObjectMapper()
                        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        final ParsedJsonSchema parsed = mapper.convertValue(jsonSchema, ParsedJsonSchema.class);

        Map<String, AgentConfigurationModel.AgentConfigurationProperty> props =
                new LinkedHashMap<>();
        if (parsed.getProperties() != null) {
            for (Map.Entry<String, ParsedJsonSchema.Prop> schema : parsed.getProperties().entrySet()) {
                final AgentConfigurationModel.AgentConfigurationProperty parsedProp =
                        parseProp(mapper, schema.getValue());
                if (parsedProp != null) {
                    props.put(schema.getKey(), parsedProp);
                }
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
            newProp.setDescription(
                    property.getDescription() == null ? null : property.getDescription().strip());
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
                        ClassConfigValidator::getPropertyNameOverrideBasedOnJsonPropertyAnnotation)
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
        JsonProperty annotation = member.getAnnotationConsideringFieldAndGetter(JsonProperty.class);
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
}
