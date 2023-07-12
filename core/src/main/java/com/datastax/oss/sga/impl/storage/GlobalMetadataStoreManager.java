package com.datastax.oss.sga.impl.storage;

import com.datastax.oss.sga.api.model.TenantConfiguration;
import com.datastax.oss.sga.api.storage.ApplicationStore;
import com.datastax.oss.sga.api.storage.GlobalMetadataStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

@AllArgsConstructor
public class GlobalMetadataStoreManager {

    private static final ObjectMapper mapper = new ObjectMapper();
    protected static final String TENANT_KEY_PREFIX = "t-";
    private final GlobalMetadataStore globalMetadataStore;
    private final ApplicationStore applicationStore;

    @SneakyThrows
    public void putTenant(String tenant, TenantConfiguration tenantConfiguration) {
        final String key = keyedTenantName(tenant);
        globalMetadataStore.put(key, mapper.writeValueAsString(tenantConfiguration));
        applicationStore.initializeTenant(tenant);
    }

    public TenantConfiguration getTenant(String tenant) {
        final String key = keyedTenantName(tenant);
        final String res = globalMetadataStore.get(key);
        if (res != null) {
            return parseTenantConfiguration(res);
        }
        return null;
    }

    @SneakyThrows
    private TenantConfiguration parseTenantConfiguration(String res) {
        return mapper.readValue(res, TenantConfiguration.class);
    }

    @SneakyThrows
    public void deleteTenant(String tenant) {
        final String key = keyedTenantName(tenant);
        globalMetadataStore.delete(key);
    }

    @SneakyThrows
    public Map<String, TenantConfiguration> listTenants() {
        return globalMetadataStore.list()
                .entrySet()
                .stream()
                .filter(e -> e.getKey().startsWith(TENANT_KEY_PREFIX))
                .collect(Collectors.toMap(
                        e -> e.getKey().substring(TENANT_KEY_PREFIX.length()),
                        e -> parseTenantConfiguration(e.getValue()))
                );
    }

    private static String keyedTenantName(String tenant) {
        return TENANT_KEY_PREFIX + tenant;
    }

}
