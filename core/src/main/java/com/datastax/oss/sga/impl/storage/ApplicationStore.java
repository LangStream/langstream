package com.datastax.oss.sga.impl.storage;

import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.model.ApplicationInstanceLifecycleStatus;
import com.datastax.oss.sga.api.model.StoredApplicationInstance;
import com.datastax.oss.sga.api.storage.ConfigStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;


@AllArgsConstructor
public class ApplicationStore {

    public static final String KEY_PREFIX = "app-";
    private static final ObjectMapper mapper = new ObjectMapper();

    private final ConfigStore configStore;

    private static String keyedName(String name) {
        return KEY_PREFIX + name;
    }

    @SneakyThrows
    public void put(String name, ApplicationInstance applicationInstance,
                    ApplicationInstanceLifecycleStatus status) {
        configStore.put(keyedName(name), mapper.writeValueAsString(StoredApplicationInstance.builder()
                .name(name)
                .instance(applicationInstance)
                .status(status)
                .build()));
    }

    @SneakyThrows
    public StoredApplicationInstance get(String key) {
        final String s = configStore.get(keyedName(key));
        if (s == null) {
            return null;
        }
        return mapper.readValue(s, StoredApplicationInstance.class);
    }

    @SneakyThrows
    public void delete(String key) {
        configStore.delete(keyedName(key));
    }

    @SneakyThrows
    public LinkedHashMap<String, StoredApplicationInstance> list() {
        return configStore.list()
                .entrySet()
                .stream()
                .filter((entry) -> entry.getKey().startsWith(KEY_PREFIX))
                .map((entry) -> {
                    try {
                        final StoredApplicationInstance parsed =
                                mapper.readValue(entry.getValue(), StoredApplicationInstance.class);
                        return Map.entry(parsed.getName(), parsed);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue(), (a, b) -> b, LinkedHashMap::new));
    }

}