package com.datastax.oss.sga.ai.agents.datasource;

import com.datastax.oss.sga.api.codestorage.CodeStorage;
import com.datastax.oss.sga.api.codestorage.CodeStorageProvider;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import com.datastax.oss.streaming.ai.model.config.DataSourceConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;

/**
 * This is the API to load a CodeStorage implementation.
 */
@Slf4j
public class DataSourceProviderRegistry {

    public static QueryStepDataSource getQueryStepDataSource(Map<String, Object> dataSourceConfig) {
        if (dataSourceConfig == null || dataSourceConfig.isEmpty()) {
            return null;
        }
        log.info("Loading DataSource implementation for {}", dataSourceConfig);

        ServiceLoader<DataSourceProvider> loader = ServiceLoader.load(DataSourceProvider.class);
        ServiceLoader.Provider<DataSourceProvider> provider = loader
                .stream()
                .filter(p -> {
                    return p.get().supports(dataSourceConfig);
                })
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No DataSource found for resource " + dataSourceConfig));

        final QueryStepDataSource implementation = provider.get().createImplementation(dataSourceConfig);
        return implementation;
    }


}
