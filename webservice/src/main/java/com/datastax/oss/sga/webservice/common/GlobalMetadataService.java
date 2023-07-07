package com.datastax.oss.sga.webservice.common;

import com.datastax.oss.sga.api.model.TenantConfiguration;
import com.datastax.oss.sga.api.storage.GlobalMetadataStore;
import com.datastax.oss.sga.api.storage.GlobalMetadataStoreRegistry;
import com.datastax.oss.sga.impl.storage.ApplicationStore;
import com.datastax.oss.sga.impl.storage.GlobalMetadataStoreManager;
import com.datastax.oss.sga.webservice.config.StorageProperties;
import java.util.Map;
import lombok.SneakyThrows;
import org.springframework.stereotype.Service;

@Service
public class GlobalMetadataService {

    private final GlobalMetadataStoreManager store;

    public GlobalMetadataService(StorageProperties storageProperties, ApplicationStore applicationStore) {
        final GlobalMetadataStore globalMetadataStore =
                GlobalMetadataStoreRegistry.loadStore(storageProperties.getGlobal().getType(),
                        storageProperties.getGlobal().getConfiguration());
        store = new GlobalMetadataStoreManager(globalMetadataStore, applicationStore);
    }

    @SneakyThrows
    public void putTenant(String tenant, TenantConfiguration tenantConfiguration) {
        store.putTenant(tenant, tenantConfiguration);
    }

    public TenantConfiguration getTenant(String tenant) {
        return store.getTenant(tenant);
    }

    @SneakyThrows
    public void deleteTenant(String tenant) {
        store.deleteTenant(tenant);
    }

    @SneakyThrows
    public Map<String, TenantConfiguration> listTenants() {
        return store.listTenants();
    }

}
