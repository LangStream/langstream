package com.datastax.oss.sga.ai.agents.datasource;

import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;

import java.util.Map;

public interface DataSourceProvider {

    boolean supports(Map<String, Object> dataSourceConfig);

    QueryStepDataSource createImplementation(Map<String, Object> dataSourceConfig);
}
