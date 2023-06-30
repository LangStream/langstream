package com.datastax.oss.sga.webservice.config;

import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "application.storage")
@Data
public class StorageProperties {
    private String type;
    private Map<String, String> configuration = new HashMap<>();

}
