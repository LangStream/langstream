package com.datastax.oss.sga.ai.agents.services;

import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import com.datastax.oss.streaming.ai.services.ServiceProvider;

import java.util.Map;

public interface ServiceProviderProvider {

    boolean supports(Map<String, Object> agentConfiguration);

    ServiceProvider createImplementation(Map<String, Object> agentConfiguration);
}
