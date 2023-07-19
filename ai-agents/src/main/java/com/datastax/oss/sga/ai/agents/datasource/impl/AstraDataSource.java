package com.datastax.oss.sga.ai.agents.datasource.impl;

import com.datastax.oss.sga.ai.agents.datasource.DataSourceProvider;
import com.datastax.oss.streaming.ai.datasource.AstraDBDataSource;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import com.datastax.oss.streaming.ai.model.config.DataSourceConfig;

import java.util.Map;

public class AstraDataSource implements DataSourceProvider {

    @Override
    public boolean supports(Map<String, Object> dataSourceConfig) {
        return "astra".equals(dataSourceConfig.get("service"));
    }

    @Override
    public QueryStepDataSource createImplementation(Map<String, Object> dataSourceConfig) {
        AstraDBDataSource result =  new AstraDBDataSource();
        DataSourceConfig implConfig = new DataSourceConfig();
        implConfig.setPassword((String) dataSourceConfig.get("password"));
        implConfig.setService("astra");
        implConfig.setUsername((String) dataSourceConfig.get("username"));
        implConfig.setSecureBundle((String) dataSourceConfig.get("secureBundle"));
        result.initialize(implConfig);
        return result;
    }
}
