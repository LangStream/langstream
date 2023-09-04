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
import com.samskivert.mustache.Mustache;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ApplicationPlaceholderResolver {

    private static final ObjectMapper mapper =
            new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

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
                                definition.setName(resolveValue(context, definition.getName()));
                                newTopics.put(resolveValue(context, topicName), definition);
                            });
            module.replaceTopics(newTopics);
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
            Gateway.Authentication authentication = gateway.authentication();
            if (gateway.authentication() != null
                    && gateway.authentication().configuration() != null) {
                authentication =
                        new Gateway.Authentication(
                                authentication.provider(),
                                resolveMap(context, gateway.authentication().configuration()));
            }
            newGateways.add(
                    new Gateway(
                            gateway.id(),
                            gateway.type(),
                            gateway.topic(),
                            authentication,
                            gateway.parameters(),
                            gateway.produceOptions(),
                            gateway.consumeOptions()));
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
                resolveValue(context, connection.definition()),
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
        } else {
            return resolveValue(context, object == null ? null : object.toString());
        }
    }

    private record Placeholder(String key, String value, String finalReplacement) {}

    static String resolveValue(Map<String, Object> context, String template) {
        List<Placeholder> placeholders = new ArrayList<>();
        placeholders.add(new Placeholder("{{% ", "{__MUSTACHE_ESCAPING_PREFIX ", "{{ "));
        placeholders.add(
                new Placeholder("{{%# ", "{__MUSTACHE_ESCAPING_PREFIX_LOOPSTART ", "{{# "));
        placeholders.add(new Placeholder("{{%/ ", "{__MUSTACHE_ESCAPING_PREFIX_LOOPEND ", "{{/ "));
        String escaped = template;
        for (Placeholder placeholder : placeholders) {
            escaped = escaped.replace(placeholder.key, placeholder.value);
        }
        final String result = Mustache.compiler().compile(escaped).execute(context);
        String finalResult = result;
        for (Placeholder placeholder : placeholders) {
            finalResult = finalResult.replace(placeholder.value, placeholder.finalReplacement);
        }
        return finalResult;
    }

    private static Application deepCopy(Application instance) throws IOException {
        return mapper.readValue(mapper.writeValueAsBytes(instance), Application.class);
    }

    private static Map<String, Object> deepCopy(Map<String, Object> context) throws IOException {
        return mapper.readValue(mapper.writeValueAsBytes(context), Map.class);
    }
}
