package com.datastax.oss.sga.webservice.config;

import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "application.storage")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class StorageProperties {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AppsStoreProperties {
        private String type;
        private Map<String, String> configuration = new HashMap<>();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SecretStoreProperties {
        private String type;
        private Map<String, String> configuration = new HashMap<>();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class GlobalMetadataStoreProperties {
        private String type;
        private Map<String, String> configuration = new HashMap<>();
    }

    private AppsStoreProperties apps;
    private SecretStoreProperties secrets;
    private GlobalMetadataStoreProperties global;

}
