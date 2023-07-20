package com.datastax.oss.sga.webservice.config;

import jakarta.validation.constraints.NotBlank;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "application.tenants")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TenantProperties {

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class DefaultTenantProperties {
        private boolean create;
        @NotBlank
        private String name;
    }

    private DefaultTenantProperties defaultTenant;

}
