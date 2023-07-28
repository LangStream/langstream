package com.datastax.oss.sga.webservice.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "application.apps")
public record ApplicationDeployProperties(
        GatewayProperties gateway
) {
    public record GatewayProperties(boolean requireAuthentication) {}
}
