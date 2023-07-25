package com.datastax.oss.sga.apigateway.application;

import com.datastax.oss.sga.api.storage.ApplicationStore;
import com.datastax.oss.sga.api.storage.ApplicationStoreRegistry;
import com.datastax.oss.sga.apigateway.config.StorageProperties;
import java.util.Objects;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ApplicationStoreFactory {

    @Bean
    public ApplicationStore getApplicationStore(StorageProperties storageProperties) {
        final StorageProperties.AppsStoreProperties apps = storageProperties.getApps();
        Objects.requireNonNull(apps);
        return ApplicationStoreRegistry.loadStore(apps.getType(), apps.getConfiguration());
    }
}