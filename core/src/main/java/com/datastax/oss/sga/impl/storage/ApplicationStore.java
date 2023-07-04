package com.datastax.oss.sga.impl.storage;

import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.model.ApplicationInstanceLifecycleStatus;
import com.datastax.oss.sga.api.model.Instance;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.Resource;
import com.datastax.oss.sga.api.model.Secrets;
import com.datastax.oss.sga.api.model.StoredApplicationInstance;
import com.datastax.oss.sga.api.storage.ConfigStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;


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

    private final Map<String, StoredApplicationInstance> localApps = new ConcurrentHashMap<>();

    public ApplicationStore(ConfigStore appConfigStore, ConfigStore secretStore) {
        this.appConfigStore = appConfigStore;
        this.secretStore = secretStore;
        loadFromStore();
    }

    private static String keyedConfigName(String name) {
        return KEY_PREFIX + name;
    }

    private static String keyedSecretName(String name) {
        return KEY_SECRET_PREFIX + name;
    }

    private void loadFromStore() {
        list();
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
        localApps.put(name, StoredApplicationInstance.builder()
                .name(name)
                .instance(applicationInstance)
                .status(status)
                .build()
        );
    }

    @SneakyThrows
    public StoredApplicationInstance get(String key) {
        return localApps.computeIfAbsent(key, k -> buildStoredApplicationInstance(k));
    }

    @SneakyThrows
    private StoredApplicationInstance buildStoredApplicationInstance(String key) {
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
        localApps.remove(key);
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
                        final StoredApplicationInstance parsed = get(unkeyed);
                        return Map.entry(parsed.getName(), parsed);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue(), (a, b) -> b, LinkedHashMap::new));
    }

}