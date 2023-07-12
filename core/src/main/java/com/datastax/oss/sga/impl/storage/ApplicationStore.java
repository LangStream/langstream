package com.datastax.oss.sga.impl.storage;

import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.ApplicationInstanceLifecycleStatus;
import com.datastax.oss.sga.api.model.Instance;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.Resource;
import com.datastax.oss.sga.api.model.Secrets;
import com.datastax.oss.sga.api.model.StoredApplication;
import com.datastax.oss.sga.api.storage.TenantDataStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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

    private final TenantDataStore appConfigStore;
    private final TenantDataStore secretStore;

    private final Map<String, Map<String, StoredApplication>> localApps = new ConcurrentHashMap<>();

    public ApplicationStore(TenantDataStore appConfigStore, TenantDataStore secretStore) {
        this.appConfigStore = appConfigStore;
        this.secretStore = secretStore;
    }

    private static String keyedConfigName(String name) {
        return KEY_PREFIX + name;
    }

    private static String keyedSecretName(String name) {
        return KEY_SECRET_PREFIX + name;
    }

    public void initializeTenant(String tenant) {
        appConfigStore.initializeTenant(tenant);
        secretStore.initializeTenant(tenant);
    }

    public void loadTenant(String tenant) {
        list(tenant);
    }

    @SneakyThrows
    public void put(String tenant, String name, Application applicationInstance,
                    ApplicationInstanceLifecycleStatus status) {
        appConfigStore.put(tenant, keyedConfigName(name), mapper.writeValueAsString(ApplicationConfigObject.builder()
                .name(name)
                .modules(applicationInstance.getModules())
                .resources(applicationInstance.getResources())
                .instance(applicationInstance.getInstance())
                .status(status)
                .build()));
        secretStore.put(tenant, keyedSecretName(name), mapper.writeValueAsString(ApplicationSecretsObject.builder()
                .secrets(applicationInstance.getSecrets())
                .build()));
        localApps.computeIfAbsent(tenant, k -> new ConcurrentHashMap<>())
                .put(name, StoredApplication.builder()
                        .name(name)
                        .instance(applicationInstance)
                        .status(status)
                        .build());

    }

    @SneakyThrows
    public StoredApplication get(String tenant, String key) {
        return localApps
                .computeIfAbsent(tenant, t -> new ConcurrentHashMap<>())
                .computeIfAbsent(key, k -> buildStoredApplicationInstance(tenant, k));
    }

    @SneakyThrows
    private StoredApplication buildStoredApplicationInstance(String tenant, String key) {
        final String s = appConfigStore.get(tenant, keyedConfigName(key));
        if (s == null) {
            return null;
        }
        final ApplicationConfigObject config = mapper.readValue(s, ApplicationConfigObject.class);
        final String secretsStr = secretStore.get(tenant, keyedSecretName(key));
        ApplicationSecretsObject secrets = null;
        if (secretsStr != null) {
            secrets = mapper.readValue(secretsStr, ApplicationSecretsObject.class);
        }
        final Application applicationInstance = new Application();
        applicationInstance.setResources(config.getResources());
        applicationInstance.setInstance(config.getInstance());
        applicationInstance.setModules(config.getModules());
        applicationInstance.setSecrets(secrets == null ? null : secrets.getSecrets());
        return StoredApplication
                .builder()
                .name(config.getName())
                .instance(applicationInstance)
                .status(config.getStatus())
                .build();
    }

    @SneakyThrows
    public void delete(String tenant, String key) {
        appConfigStore.delete(tenant, keyedConfigName(key));
        secretStore.delete(tenant, keyedSecretName(key));
        final Map<String, StoredApplication> tenantApps = localApps.get(tenant);
        if (tenantApps != null) {
            tenantApps.remove(key);
        }
    }

    @SneakyThrows
    public LinkedHashMap<String, StoredApplication> list(String tenant) {
        return appConfigStore.list(tenant)
                .entrySet()
                .stream()
                .filter((entry) -> entry.getKey().startsWith(KEY_PREFIX))
                .map((entry) -> {
                    try {
                        final String unkeyed = entry.getKey().substring(KEY_PREFIX.length());
                        final StoredApplication parsed = get(tenant, unkeyed);
                        return Map.entry(parsed.getName(), parsed);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue(), (a, b) -> b, LinkedHashMap::new));
    }

}