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
package ai.langstream.webservice.doc;

import ai.langstream.api.doc.AgentConfigurationModel;
import ai.langstream.api.doc.AgentsConfigurationModel;
import ai.langstream.api.runtime.AgentNodeProvider;
import ai.langstream.api.runtime.PluginsRegistry;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DocumentationGenerator {

    public static void generateAgentDocsToFile(Path file, String version) throws Exception {}

    public static AgentsConfigurationModel generateAgentDocs(String version) {
        Map<String, AgentConfigurationModel> agents = new TreeMap<>();
        final List<AgentNodeProvider> nodes =
                new PluginsRegistry().lookupAvailableAgentImplementations(null);
        for (AgentNodeProvider node : nodes) {
            agents.putAll(node.generateSupportedTypesDocumentation());
        }
        return new AgentsConfigurationModel(version, agents);
    }

    /*
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
                           AgentDocumentationGeneratorV2
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
    */

}
