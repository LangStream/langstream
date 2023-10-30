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
package ai.langstream.impl.common;

import ai.langstream.api.model.AgentConfiguration;
import ai.langstream.api.model.Application;
import ai.langstream.api.model.ComputeCluster;
import ai.langstream.api.model.Connection;
import ai.langstream.api.model.Gateway;
import ai.langstream.api.model.Gateways;
import ai.langstream.api.model.Instance;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.Pipeline;
import ai.langstream.api.model.Resource;
import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.model.TopicDefinition;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ApplicationPlaceholderResolver {

    private static final ObjectMapper mapper =
            new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    private static final ObjectMapper mapperForTemplates =
            new ObjectMapper()
                    .configure(
                            SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS,
                            true); // help with tests (also of applications using LS)

    private ApplicationPlaceholderResolver() {}

    @SneakyThrows
    public static Application resolvePlaceholders(Application instance) {
        instance = deepCopy(instance);
        final Map<String, Object> context = createContext(instance);
        if (log.isDebugEnabled()) {
            log.debug(
                    "Resolving placeholders with context:\n{}", mapper.writeValueAsString(context));
        }
        if (log.isDebugEnabled()) {
            log.debug("Resolve context: {}", context);
        }

        instance.setInstance(resolveInstance(instance, context));
        instance.setResources(resolveResources(instance, context));
        instance.setModules(resolveModules(instance, context));
        instance.setGateways(resolveGateways(instance, context));
        return instance;
    }

    static Map<String, Object> createContext(Application application) throws IOException {
        Map<String, Object> context = new HashMap<>();
        final Instance instance = application.getInstance();
        if (instance != null) {
            context.put("cluster", instance.streamingCluster());
            context.put("globals", instance.globals());
        }

        Map<String, Map<String, Object>> secrets = new HashMap<>();
        if (application.getSecrets() != null && application.getSecrets().secrets() != null) {
            application.getSecrets().secrets().forEach((k, v) -> secrets.put(k, v.data()));
        }
        context.put("secrets", secrets);
        context = deepCopy(context);
        return context;
    }

    private static Map<String, Module> resolveModules(
            Application instance, Map<String, Object> context) {
        Map<String, Module> newModules = new LinkedHashMap<>();
        for (Map.Entry<String, Module> moduleEntry : instance.getModules().entrySet()) {
            final Module module = moduleEntry.getValue();
            Map<String, TopicDefinition> newTopics = new HashMap<>();
            module.getTopics()
                    .entrySet()
                    .forEach(
                            (Map.Entry<String, TopicDefinition> entry) -> {
                                String topicName = entry.getKey();
                                TopicDefinition definition = entry.getValue().copy();
                                definition.setName(
                                        resolveValueAsString(context, definition.getName()));
                                newTopics.put(resolveValueAsString(context, topicName), definition);
                            });
            module.replaceTopics(newTopics);
            if (module.getAssets() != null) {
                module.getAssets()
                        .forEach(
                                asset -> {
                                    asset.setConfig(resolveMap(context, asset.getConfig()));
                                });
            }

            for (Map.Entry<String, Pipeline> pipelineEntry : module.getPipelines().entrySet()) {
                final Pipeline pipeline = pipelineEntry.getValue();
                List<AgentConfiguration> newAgents = new ArrayList<>();
                for (AgentConfiguration value : pipeline.getAgents()) {
                    value.setConfiguration(resolveMap(context, value.getConfiguration()));
                    value.setInput(resolveConnection(context, value.getInput()));
                    value.setOutput(resolveConnection(context, value.getOutput()));
                    newAgents.add(value);
                }
                pipeline.setAgents(newAgents);
            }
            newModules.put(moduleEntry.getKey(), module);
        }
        return newModules;
    }

    private static Instance resolveInstance(
            Application applicationInstance, Map<String, Object> context) {
        final StreamingCluster newCluster;
        final ComputeCluster newComputeCluster;
        final Instance instance = applicationInstance.getInstance();
        if (instance == null) {
            return null;
        }
        final StreamingCluster cluster = instance.streamingCluster();
        if (cluster != null) {
            newCluster =
                    new StreamingCluster(
                            cluster.type(), resolveMap(context, cluster.configuration()));
        } else {
            newCluster = null;
        }
        final ComputeCluster computeCluster = instance.computeCluster();
        if (computeCluster != null) {
            newComputeCluster =
                    new ComputeCluster(
                            computeCluster.type(),
                            resolveMap(context, computeCluster.configuration()));
        } else {
            newComputeCluster = null;
        }
        return new Instance(newCluster, newComputeCluster, resolveMap(context, instance.globals()));
    }

    private static Map<String, Resource> resolveResources(
            Application instance, Map<String, Object> context) {
        Map<String, Resource> newResources = new HashMap<>();
        for (Map.Entry<String, Resource> resourceEntry : instance.getResources().entrySet()) {
            final Resource resource = resourceEntry.getValue();
            newResources.put(
                    resourceEntry.getKey(),
                    new Resource(
                            resource.id(),
                            resource.name(),
                            resource.type(),
                            resolveMap(context, resource.configuration())));
        }
        return newResources;
    }

    private static Gateways resolveGateways(Application instance, Map<String, Object> context) {
        if (instance.getGateways() == null || instance.getGateways().gateways() == null) {
            return instance.getGateways();
        }
        List<Gateway> newGateways = new ArrayList<>();
        for (Gateway gateway : instance.getGateways().gateways()) {
            Gateway.Authentication authentication = gateway.getAuthentication();
            if (gateway.getAuthentication() != null
                    && gateway.getAuthentication().getConfiguration() != null) {
                authentication =
                        new Gateway.Authentication(
                                authentication.getProvider(),
                                resolveMap(context, gateway.getAuthentication().getConfiguration()),
                                authentication.isAllowTestMode());
            }

            final String topic = resolveValueAsString(context, gateway.getTopic());
            final String eventsTopic = resolveValueAsString(context, gateway.getEventsTopic());
            newGateways.add(
                    new Gateway(
                            gateway.getId(),
                            gateway.getType(),
                            topic,
                            authentication,
                            gateway.getParameters(),
                            gateway.getProduceOptions(),
                            gateway.getConsumeOptions(),
                            gateway.getChatOptions(),
                            gateway.getServiceOptions(),
                            eventsTopic));
        }
        return new Gateways(newGateways);
    }

    static Map<String, Object> resolveMap(Map<String, Object> context, Map<String, Object> config) {
        Map<String, Object> resolvedConfig = new HashMap<>();
        if (config == null) {
            return resolvedConfig;
        }
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            resolvedConfig.put(entry.getKey(), resolveValue(context, entry.getValue()));
        }
        return resolvedConfig;
    }

    static Connection resolveConnection(Map<String, Object> context, Connection connection) {
        if (connection == null) {
            return null;
        }
        return new Connection(
                connection.connectionType(),
                resolveValueAsString(context, connection.definition()),
                connection.enableDeadletterQueue());
    }

    static Collection<?> resolveCollection(Map<String, Object> context, Collection<?> config) {
        return config.stream()
                .map(entry -> resolveValue(context, entry))
                .collect(Collectors.toList());
    }

    static Object resolveValue(Map<String, Object> context, Object object) {
        if (object instanceof Map) {
            return resolveMap(context, (Map<String, Object>) object);
        } else if (object instanceof Collection) {
            return resolveCollection(context, (Collection<?>) object);
        } else if (object instanceof String) {
            return resolveSingleValue(context, (String) object);
        } else {
            return object;
        }
    }

    static String resolveValueAsString(Map<String, Object> context, String template) {
        Object value = resolveSingleValue(context, template);
        return value == null ? null : value.toString();
    }

    static Object resolveSingleValue(Map<String, Object> context, String template) {
        if (template == null) {
            return null;
        }
        String reference = template.trim();
        if (!reference.startsWith("${") || !reference.endsWith("}")) {
            // this is a raw value like    "username=${secrets.username}"
            // password=${secrets.password}"
            return resolvePlaceholdersInString(template, context);
        }

        // exact match ${ x.y.z } (and not ${ x.y.z }${ x.y.z })
        if (reference.startsWith("${")
                && reference.endsWith("}")
                && reference.indexOf("{") == reference.lastIndexOf("{")) {
            String placeholder = reference.substring(2, reference.length() - 1);
            return resolveReference(placeholder, context);
        }
        log.warn("Unknown placeholder: {}", reference);
        return resolvePlaceholdersInString(template, context);
    }

    private static Set<String> ONLY_SECRETS_AND_GLOBALS = Set.of("secrets", "globals");

    static String resolvePlaceholdersInString(String template, Map<String, Object> context) {
        if (!template.contains("${") && template.contains("{{") && template.contains("}}")) {
            // 0.x syntax
            if (template.contains("{{{")) {
                return resolvePlaceholdersInString(
                        template, context, "{{{", "}}}", ONLY_SECRETS_AND_GLOBALS);
            } else {
                return resolvePlaceholdersInString(
                        template, context, "{{", "}}", ONLY_SECRETS_AND_GLOBALS);
            }
        }
        return resolvePlaceholdersInString(template, context, "${", "}", null);
    }

    private static String resolvePlaceholdersInString(
            String template,
            Map<String, Object> context,
            String prefix,
            String suffix,
            Set<String> allowedRoots) {
        StringBuilder result = new StringBuilder();
        int position = 0;
        int pos = template.indexOf(prefix, position);
        if (pos < 0) {
            return template;
        }
        while (pos >= 0) {
            result.append(template, position, pos);
            int end = template.indexOf(suffix, pos);
            if (end < 0) {
                throw new IllegalArgumentException("Invalid placeholder: " + template);
            }
            String placeholder = template.substring(pos + prefix.length(), end).trim();

            if (allowedRoots != null && allowedRoots.stream().noneMatch(placeholder::startsWith)) {
                // this is a raw value like "The question is {{{value.question}}}"
                // in a mustache template
                // at this step of the preprocessor we allow mustache like syntax only for secrets
                // and globals
                // in order to keep compatibility with 0.x applications
                return template;
            }
            Object value = resolveReference(placeholder, context);
            if (value == null) {
                // to not write "null" inside the string
                value = "";
            }
            if (!(value instanceof String)) {
                // stuff that is not a String has to be converted to something that fits in a String
                // using JSON is the least bad option
                try {
                    value = mapperForTemplates.writeValueAsString(value);
                } catch (IOException impossible) {
                    throw new IllegalStateException(impossible);
                }
            }
            result.append(value);
            position = end + suffix.length();
            pos = template.indexOf(prefix, position);
        }
        result.append(template, position, template.length());
        return result.toString();
    }

    private static Object resolveReference(String placeholder, Object context) {
        try {
            placeholder = placeholder.trim();
            int dot = placeholder.indexOf('.');
            if (dot < 0) {
                return resolveProperty(context, placeholder);
            } else {
                String parent = placeholder.substring(0, dot);
                String child = placeholder.substring(dot + 1);
                Object parentValue = resolveProperty(context, parent);
                if (parentValue == null) {
                    throw new IllegalArgumentException(
                            "Cannot resolve property " + parent + " in context " + context);
                }
                return resolveReference(child, parentValue);
            }
        } catch (IllegalArgumentException error) {
            throw new IllegalArgumentException("Cannot resolve reference " + placeholder, error);
        }
    }

    private static Object resolveProperty(Object context, String property) {
        if (context == null) {
            throw new IllegalArgumentException(
                    "Property " + property + " cannot be resolved on a empty context");
        }
        if (context instanceof Map) {
            return ((Map) context).get(property);
        } else {
            throw new IllegalArgumentException(
                    "Cannot resolve property " + property + " on " + context);
        }
    }

    private static Application deepCopy(Application instance) throws IOException {
        return mapper.readValue(mapper.writeValueAsBytes(instance), Application.class);
    }

    private static Map<String, Object> deepCopy(Map<String, Object> context) throws IOException {
        return mapper.readValue(mapper.writeValueAsBytes(context), Map.class);
    }
}
