package com.datastax.oss.sga.impl.storage;

import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.model.ApplicationInstanceLifecycleStatus;
import com.datastax.oss.sga.api.model.Instance;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.Resource;
import com.datastax.oss.sga.api.model.Secrets;
import com.datastax.oss.sga.api.model.StoredApplicationInstance;
import com.datastax.oss.sga.api.storage.ConfigStore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;


@AllArgsConstructor
public class ApplicationStore {

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ApplicationConfigObject {
        private String name;
        private Map<String, Resource> resources;
        private Map<String, Module> modules;
        private Instance instance;
        private ApplicationInstanceLifecycleStatus status;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ApplicationSecretsObject {
        private Secrets secrets;
    }

    public static final String KEY_PREFIX = "app-";
    public static final String KEY_SECRET_PREFIX = "sec-";
    private static final ObjectMapper mapper = new ObjectMapper();

    private final ConfigStore appConfigStore;
    private final ConfigStore secretStore;

    private static String keyedConfigName(String name) {
        return KEY_PREFIX + name;
    }

    private static String keyedSecretName(String name) {
        return KEY_SECRET_PREFIX + name;
    }

    @SneakyThrows
    public void put(String name, ApplicationInstance applicationInstance,
                    ApplicationInstanceLifecycleStatus status) {
        appConfigStore.put(keyedConfigName(name), mapper.writeValueAsString(ApplicationConfigObject.builder()
                .name(name)
                .modules(applicationInstance.getModules())
                .resources(applicationInstance.getResources())
                .instance(applicationInstance.getInstance())
                .status(status)
                .build()));
        secretStore.put(keyedSecretName(name), mapper.writeValueAsString(ApplicationSecretsObject.builder()
                .secrets(applicationInstance.getSecrets())
                .build()));
    }

    @SneakyThrows
    public StoredApplicationInstance get(String key) {
        return buildStoredApplicationInstance(key);

    }

    private StoredApplicationInstance buildStoredApplicationInstance(String key)
            throws JsonProcessingException {
        final String s = appConfigStore.get(keyedConfigName(key));
        if (s == null) {
            return null;
        }
        final ApplicationConfigObject config = mapper.readValue(s, ApplicationConfigObject.class);
        final String secretsStr = secretStore.get(keyedSecretName(key));
        ApplicationSecretsObject secrets = null;
        if (secretsStr != null) {
            secrets = mapper.readValue(secretsStr, ApplicationSecretsObject.class);
        }
        final ApplicationInstance applicationInstance = new ApplicationInstance();
        applicationInstance.setResources(config.getResources());
        applicationInstance.setInstance(config.getInstance());
        applicationInstance.setModules(config.getModules());
        applicationInstance.setSecrets(secrets == null ? null : secrets.getSecrets());
        return StoredApplicationInstance
                .builder()
                .name(config.getName())
                .instance(applicationInstance)
                .status(config.getStatus())
                .build();
    }

    @SneakyThrows
    public void delete(String key) {
        appConfigStore.delete(keyedConfigName(key));
        secretStore.delete(keyedSecretName(key));
    }

    @SneakyThrows
    public LinkedHashMap<String, StoredApplicationInstance> list() {
        return appConfigStore.list()
                .entrySet()
                .stream()
                .filter((entry) -> entry.getKey().startsWith(KEY_PREFIX))
                .map((entry) -> {
                    try {
                        final String unkeyed = entry.getKey().substring(KEY_PREFIX.length());
                        final StoredApplicationInstance parsed = buildStoredApplicationInstance(unkeyed);
                        return Map.entry(parsed.getName(), parsed);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue(), (a, b) -> b, LinkedHashMap::new));
    }

}