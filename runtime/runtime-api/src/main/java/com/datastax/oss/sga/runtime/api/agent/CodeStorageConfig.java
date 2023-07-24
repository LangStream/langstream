package com.datastax.oss.sga.runtime.api.agent;

import java.util.Map;

/**
 * Configuration for the code storage on the Pod Runtime.
 * This is used to download the custom code for the agent.
 * @param type the type (i.e. "S3");
 * @param configuration the configuration
 */
public record CodeStorageConfig(String tenant, String type, String codeStorageArchiveId, Map<String, Object> configuration){
}
