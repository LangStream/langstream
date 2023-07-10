package com.datastax.oss.sga.impl.common;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.model.ComputeCluster;
import com.datastax.oss.sga.api.model.Instance;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.Pipeline;
import com.datastax.oss.sga.api.model.Resource;
import com.datastax.oss.sga.api.model.StreamingCluster;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.samskivert.mustache.Mustache;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ApplicationInstancePlaceholderResolver {

    private static final ObjectMapper mapper = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    private ApplicationInstancePlaceholderResolver() {
    }

    @SneakyThrows
    public static ApplicationInstance resolvePlaceholders(ApplicationInstance instance) {
        instance = deepCopy(instance);
        final Map<String, Object> context = createContext(instance);
        if (log.isDebugEnabled()) {
            log.debug("Resolving placeholders with context:\n{}", mapper.writeValueAsString(context));
        }


        instance.setInstance(resolveInstance(instance, context));
        instance.setResources(resolveResources(instance, context));
        instance.setModules(resolveModules(instance, context));
        return instance;
    }

    static Map<String, Object> createContext(ApplicationInstance application) throws IOException {
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

    private static Map<String, Module> resolveModules(ApplicationInstance instance, Map<String, Object> context) {
        Map<String, Module> newModules = new LinkedHashMap<>();
        for (Map.Entry<String, Module> moduleEntry : instance.getModules().entrySet()) {
            final Module module = moduleEntry.getValue();
            for (Map.Entry<String, Pipeline> pipelineEntry : module.getPipelines().entrySet()) {
                final Pipeline pipeline = pipelineEntry.getValue();
                Map<String, AgentConfiguration> newAgents = new LinkedHashMap<>();
                for (Map.Entry<String, AgentConfiguration> stringAgentConfigurationEntry : pipeline.getAgents()
                        .entrySet()) {
                    final AgentConfiguration value = stringAgentConfigurationEntry.getValue();
                    value.setConfiguration(resolveMap(context, value.getConfiguration()));
                    newAgents.put(stringAgentConfigurationEntry.getKey(), value);
                }
                pipeline.setAgents(newAgents);
            }
            newModules.put(moduleEntry.getKey(), module);
        }
        return newModules;
    }

    private static Instance resolveInstance(ApplicationInstance applicationInstance, Map<String, Object> context) {
        final StreamingCluster newCluster;
        final ComputeCluster newComputeCluster;
        final Instance instance = applicationInstance.getInstance();
        if (instance == null) {
            return null;
        }
        final StreamingCluster cluster = instance.streamingCluster();
        if (cluster != null) {
            newCluster = new StreamingCluster(cluster.type(), resolveMap(context, cluster.configuration()));
        } else {
            newCluster = null;
        }
        final ComputeCluster computeCluster = instance.computeCluster();
        if (computeCluster != null) {
            newComputeCluster = new ComputeCluster(computeCluster.type(), resolveMap(context, computeCluster.configuration()));
        } else {
            newComputeCluster = null;
        }
        return new Instance(
                newCluster,
                newComputeCluster,
                resolveMap(context, instance.globals())
        );
    }

    private static Map<String, Resource> resolveResources(ApplicationInstance instance,
                                                          Map<String, Object> context) {
        Map<String, Resource> newResources = new HashMap<>();
        for (Map.Entry<String, Resource> resourceEntry : instance.getResources().entrySet()) {
            final Resource resource = resourceEntry.getValue();
            newResources.put(resourceEntry.getKey(),
                    new Resource(
                            resource.id(),
                            resource.name(),
                            resource.type(),
                            resolveMap(context, resource.configuration())
                    )
            );
        }
        return newResources;
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

    static Collection<?> resolveCollection(Map<String, Object> context, Collection<?> config) {
        return config.stream()
                .map(entry -> {
                    return resolveValue(context, entry);
                })
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

    static String resolveValue(Map<String, Object> context, String template) {
        final String escaped = template.replace("{{% ", "{__MUSTACHE_ESCAPING_PREFIX ");
        final String result = Mustache.compiler()
                .compile(escaped)
                .execute(context);
        return result.
                replace("{__MUSTACHE_ESCAPING_PREFIX ", "{{ ");
    }

    private static ApplicationInstance deepCopy(ApplicationInstance instance) throws IOException {
        return mapper.readValue(mapper.writeValueAsBytes(instance), ApplicationInstance.class);
    }

    private static Map<String, Object> deepCopy(Map<String, Object> context) throws IOException {
        return mapper.readValue(mapper.writeValueAsBytes(context), Map.class);
    }
}
