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

import ai.langstream.ai.agents.commons.jstl.JstlEvaluator;
import ai.langstream.api.doc.AgentConfig;
import ai.langstream.api.doc.AgentConfigurationModel;
import ai.langstream.api.doc.AssetConfig;
import ai.langstream.api.doc.AssetConfigurationModel;
import ai.langstream.api.doc.ConfigProperty;
import ai.langstream.api.doc.ConfigPropertyIgnore;
import ai.langstream.api.doc.ConfigPropertyModel;
import ai.langstream.api.doc.ExtendedValidationType;
import ai.langstream.api.doc.ResourceConfig;
import ai.langstream.api.doc.ResourceConfigurationModel;
import ai.langstream.api.model.AgentConfiguration;
import ai.langstream.api.model.AssetDefinition;
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
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

@Slf4j
public class ClassConfigValidator {

    static final ObjectMapper jsonWriter =
            new ObjectMapper()
                    .configure(SerializationFeature.INDENT_OUTPUT, true)
                    .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    static final ObjectMapper validatorMapper =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    static final Map<String, AgentConfigurationModel> agentModels = new ConcurrentHashMap<>();
    static final Map<String, ResourceConfigurationModel> resourceModels = new ConcurrentHashMap<>();
    static final Map<String, AssetConfigurationModel> assetModels = new ConcurrentHashMap<>();

    public static <T> T convertValidatedConfiguration(
            Map<String, Object> agentConfiguration, Class<T> clazz) {
        return validatorMapper.convertValue(agentConfiguration, clazz);
    }

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

    public static AssetConfigurationModel generateAssetModelFromClass(Class clazz) {
        return assetModels.computeIfAbsent(
                clazz.getName(), k -> generateAssetFromClassNoCache(clazz));
    }

    private static AssetConfigurationModel generateAssetFromClassNoCache(Class clazz) {
        AssetConfigurationModel model = new AssetConfigurationModel();

        final AssetConfig assetConfig = (AssetConfig) clazz.getAnnotation(AssetConfig.class);
        if (assetConfig != null) {
            if (assetConfig.description() != null && !assetConfig.description().isBlank()) {
                model.setDescription(assetConfig.description().strip());
            }
            if (assetConfig.name() != null && !assetConfig.name().isBlank()) {
                model.setName(assetConfig.name());
            }
        }
        model.setProperties(readPropertiesFromClass(clazz));
        return model;
    }

    public interface EntityRef {
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
        validateModelFromClass(
                new AgentEntityRef(agentConfiguration), modelClazz, asMap, allowUnknownProperties);
    }

    @AllArgsConstructor
    public static class ResourceEntityRef implements EntityRef {

        private final Resource resource;

        @Override
        public String ref() {
            return "resource configuration (resource: '%s', type: '%s')"
                    .formatted(
                            resource.name() == null ? resource.id() : resource.name(),
                            resource.type());
        }
    }

    @SneakyThrows
    public static void validateResourceModelFromClass(
            Resource resource,
            Class modelClazz,
            Map<String, Object> asMap,
            boolean allowUnknownProperties) {
        validateModelFromClass(
                new ResourceEntityRef(resource), modelClazz, asMap, allowUnknownProperties);
    }

    @AllArgsConstructor
    public static class AssetEntityRef implements EntityRef {

        private final AssetDefinition asset;

        @Override
        public String ref() {
            return "asset configuration (asset: '%s', type: '%s')"
                    .formatted(
                            asset.getName() == null ? asset.getId() : asset.getName(),
                            asset.getAssetType());
        }
    }

    @AllArgsConstructor
    public static class AgentEntityRef implements EntityRef {

        private final AgentConfiguration agentConfiguration;

        @Override
        public String ref() {
            return "agent configuration (agent: '%s', type: '%s')"
                    .formatted(
                            agentConfiguration.getName() == null
                                    ? agentConfiguration.getId()
                                    : agentConfiguration.getName(),
                            agentConfiguration.getType());
        }
    }

    @SneakyThrows
    public static void validateAssetModelFromClass(
            AssetDefinition asset,
            Class modelClazz,
            Map<String, Object> asMap,
            boolean allowUnknownProperties) {
        validateModelFromClass(
                new AssetEntityRef(asset), modelClazz, asMap, allowUnknownProperties);
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

        validateConversion(asMap, modelClazz, entityRef);
    }

    public static String formatErrString(EntityRef entityRef, String property, String message) {
        return "Found error on %s. Property '%s' %s".formatted(entityRef.ref(), property, message);
    }

    public static String formatErrString(EntityRef entityRef, String message) {
        return "Found error on %s. %s".formatted(entityRef.ref(), message);
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
                            formatErrString(
                                    entityRef,
                                    fullPropertyKey,
                                    "is unknown, you may want to try with some of "
                                            + properties.keySet()));
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
        if (propertyValue.getExtendedValidationType() != null) {
            validateExtendedValidationType(propertyValue.getExtendedValidationType(), actualValue);
        }

        if (propertyValue.getItems() != null && actualValue != null) {
            if (actualValue instanceof Collection collection) {
                for (Object o : collection) {
                    validateProperty(
                            entityRef, fullPropertyKey, o, propertyValue.getItems(), propertyKey);
                }
            } else {
                validateProperty(
                        entityRef,
                        fullPropertyKey,
                        actualValue,
                        propertyValue.getItems(),
                        propertyKey);
            }
        }
    }

    @Data
    public static class ConfigPropertyModel {
        private boolean ignore;
        private boolean required;
        private String description;
        private String defaultValue;
        private ExtendedValidationType extendedValidationType;
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
            if (property.getExtendedValidationType() != null
                    && property.getExtendedValidationType() != ExtendedValidationType.NONE) {
                newProp.setExtendedValidationType(property.getExtendedValidationType());
            }
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
                                        if (annotation.extendedValidationType() != null) {
                                            model.setExtendedValidationType(
                                                    annotation.extendedValidationType());
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

    @SneakyThrows
    public static Map<String, Object> validateGenericClassAndApplyDefaults(
            EntityRef entityRef,
            Class modelClazz,
            Map<String, Object> inputValue,
            boolean allowUnknownProperties) {

        final Map asMap =
                validatorMapper.readValue(validatorMapper.writeValueAsBytes(inputValue), Map.class);

        final Map<String, ai.langstream.api.doc.ConfigPropertyModel> properties =
                readPropertiesFromClass(modelClazz);

        validateProperties(entityRef, null, asMap, properties, allowUnknownProperties);

        validateConversion(asMap, modelClazz, entityRef);

        Map<String, Object> result = new LinkedHashMap<>();

        properties
                .entrySet()
                .forEach(
                        e -> {
                            Object value = inputValue.get(e.getKey());
                            if (value == null
                                    && !e.getValue().isRequired()
                                    && e.getValue().getDefaultValue() != null) {
                                value = e.getValue().getDefaultValue();
                            }
                            if (value != null) {
                                result.put(e.getKey(), value);
                            }
                        });
        return result;
    }

    private static void validateConversion(
            Map<String, Object> asMap, Class modelClazz, EntityRef entityRef) {
        try {
            convertValidatedConfiguration(asMap, modelClazz);
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

    private static void validateExtendedValidationType(
            ExtendedValidationType extendedValidationType, Object actualValue) {
        switch (extendedValidationType) {
            case EL_EXPRESSION -> {
                if (actualValue instanceof String expression) {
                    log.info("Validating EL expression: {}", expression);
                    new JstlEvaluator("${" + actualValue + "}", Object.class);
                } else if (actualValue instanceof Collection collection) {
                    log.info("Validating EL expressions {}", collection);
                    for (Object o : collection) {
                        if (o == null) {
                            throw new IllegalArgumentException(
                                    "A null value is not allowed in a list of EL expressions");
                        }
                        new JstlEvaluator(o.toString(), Object.class);
                    }
                }
            }
            case NONE -> {}
        }
    }
}
