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
import ai.langstream.api.doc.ResourceConfig;
import ai.langstream.api.doc.ResourceConfigurationModel;
import ai.langstream.api.model.AgentConfiguration;
import ai.langstream.api.model.Resource;
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
    static final Map<String, ResourceConfigurationModel> resourceModels = new ConcurrentHashMap<>();

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

    public static ResourceConfigurationModel generateResourceModelFromClass(Class clazz) {
        return resourceModels.computeIfAbsent(
                clazz.getName(), k -> generateResourceModelFromClassNoCache(clazz));
    }

    private static ResourceConfigurationModel generateResourceModelFromClassNoCache(Class clazz) {
        ResourceConfigurationModel model = new ResourceConfigurationModel();

        final ResourceConfig resourceConfig =
                (ResourceConfig) clazz.getAnnotation(ResourceConfig.class);
        if (resourceConfig != null) {
            if (resourceConfig.description() != null && !resourceConfig.description().isBlank()) {
                model.setDescription(resourceConfig.description().strip());
            }
            if (resourceConfig.name() != null && !resourceConfig.name().isBlank()) {
                model.setName(resourceConfig.name());
            }
        }
        model.setProperties(readPropertiesFromClass(clazz));
        return model;
    }

    interface EntityRef {
        String ref();
    }

    @SneakyThrows
    public static void validateAgentModelFromClass(
            AgentConfiguration agentConfiguration, Class modelClazz, Map<String, Object> asMap) {
        validateAgentModelFromClass(agentConfiguration, modelClazz, asMap, false);
    }

    @SneakyThrows
    public static void validateAgentModelFromClass(
            AgentConfiguration agentConfiguration,
            Class modelClazz,
            Map<String, Object> asMap,
            boolean allowUnknownProperties) {
        final EntityRef ref =
                () ->
                        "agent configuration (agent: '%s', type: '%s')"
                                .formatted(
                                        agentConfiguration.getName() == null
                                                ? agentConfiguration.getId()
                                                : agentConfiguration.getName(),
                                        agentConfiguration.getType());
        validateModelFromClass(ref, modelClazz, asMap, allowUnknownProperties);
    }

    @SneakyThrows
    public static void validateResourceModelFromClass(
            Resource resource,
            Class modelClazz,
            Map<String, Object> asMap,
            boolean allowUnknownProperties) {
        final EntityRef ref =
                () ->
                        "resource configuration (resource: '%s', type: '%s')"
                                .formatted(
                                        resource.name() == null ? resource.id() : resource.name(),
                                        resource.type());
        validateModelFromClass(ref, modelClazz, asMap, allowUnknownProperties);
    }

    @SneakyThrows
    private static void validateModelFromClass(
            EntityRef entityRef,
            Class modelClazz,
            Map<String, Object> asMap,
            boolean allowUnknownProperties) {
        asMap = validatorMapper.readValue(validatorMapper.writeValueAsBytes(asMap), Map.class);

        final AgentConfigurationModel agentConfigurationModel =
                generateAgentModelFromClass(modelClazz);

        validateProperties(
                entityRef,
                null,
                asMap,
                agentConfigurationModel.getProperties(),
                allowUnknownProperties);

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
                                entityRef,
                                property,
                                "has a wrong data type. Expected type: "
                                        + mismatchedInputException.getTargetType().getName()));
            } else {
                throw ex;
            }
        }
    }

    private static String formatErrString(EntityRef entityRef, String property, String message) {
        return "Found error on %s. Property '%s' %s".formatted(entityRef.ref(), property, message);
    }

    private static void validateProperties(
            EntityRef entityRef,
            String parentProp,
            Map<String, Object> asMap,
            Map<String, ai.langstream.api.doc.ConfigPropertyModel> properties,
            boolean allowUnknownProperties) {
        if (!allowUnknownProperties && asMap != null) {
            for (String key : asMap.keySet()) {
                if (!properties.containsKey(key)) {
                    final String fullPropertyKey =
                            parentProp == null ? key : parentProp + "." + key;
                    throw new IllegalArgumentException(
                            formatErrString(entityRef, fullPropertyKey, "is unknown"));
                }
            }
        }

        for (Map.Entry<String, ai.langstream.api.doc.ConfigPropertyModel> property :
                properties.entrySet()) {
            final ai.langstream.api.doc.ConfigPropertyModel propertyValue = property.getValue();
            final String propertyKey = property.getKey();
            validateProperty(
                    entityRef,
                    parentProp,
                    asMap == null ? null : asMap.get(propertyKey),
                    propertyValue,
                    propertyKey);
        }
    }

    private static void validateProperty(
            EntityRef entityRef,
            String parentProp,
            Object actualValue,
            ai.langstream.api.doc.ConfigPropertyModel propertyValue,
            String propertyKey) {

        final String fullPropertyKey =
                parentProp == null ? propertyKey : parentProp + "." + propertyKey;

        if (propertyValue.isRequired()) {
            if (actualValue == null) {
                throw new IllegalArgumentException(
                        formatErrString(entityRef, fullPropertyKey, "is required"));
            }
        }
        if (propertyValue.getProperties() != null) {
            validateProperties(
                    entityRef,
                    fullPropertyKey,
                    actualValue == null ? null : (Map<String, Object>) actualValue,
                    propertyValue.getProperties(),
                    false);
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

    public static Map<String, ai.langstream.api.doc.ConfigPropertyModel> readPropertiesFromClass(
            Class clazz) {
        JsonNode jsonSchema = getJsonSchema(clazz);
        final ObjectMapper mapper =
                new ObjectMapper()
                        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        final ParsedJsonSchema parsed = mapper.convertValue(jsonSchema, ParsedJsonSchema.class);

        Map<String, ai.langstream.api.doc.ConfigPropertyModel> props = new LinkedHashMap<>();
        if (parsed.getProperties() != null) {
            for (Map.Entry<String, ParsedJsonSchema.Prop> schema :
                    parsed.getProperties().entrySet()) {
                final ai.langstream.api.doc.ConfigPropertyModel parsedProp =
                        parseProp(mapper, schema.getValue());
                if (parsedProp != null) {
                    props.put(schema.getKey(), parsedProp);
                }
            }
        }
        return props;
    }

    @SneakyThrows
    private static ai.langstream.api.doc.ConfigPropertyModel parseProp(
            ObjectMapper mapper, ParsedJsonSchema.Prop value) {
        ai.langstream.api.doc.ConfigPropertyModel newProp =
                new ai.langstream.api.doc.ConfigPropertyModel();

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
            final ai.langstream.api.doc.ConfigPropertyModel items =
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
