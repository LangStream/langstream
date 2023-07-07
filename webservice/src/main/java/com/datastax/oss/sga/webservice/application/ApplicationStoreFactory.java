package com.datastax.oss.sga.webservice.application;

import com.datastax.oss.sga.api.storage.TenantDataStore;
import com.datastax.oss.sga.api.storage.TenantDataStoreRegistry;
import com.datastax.oss.sga.impl.storage.ApplicationStore;
import com.datastax.oss.sga.webservice.config.StorageProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ApplicationStoreFactory {

    @Bean
    public ApplicationStore getApplicationStore(StorageProperties storageProperties) {
        final TenantDataStore configStore =
                TenantDataStoreRegistry.loadAppsStore(storageProperties.getApps().getType(),
                        storageProperties.getApps().getConfiguration());
        final TenantDataStore secretsStore =
                TenantDataStoreRegistry.loadSecretsStore(storageProperties.getSecrets().getType(),
                        storageProperties.getSecrets().getConfiguration());
        final ApplicationStore store = new ApplicationStore(configStore, secretsStore);
        return store;
    }
}
