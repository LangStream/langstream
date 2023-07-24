package com.datastax.oss.sga.webservice.config;

import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "application.security.token")
public record AuthTokenProperties(
        String secretKey,
        String publicKey,
        String authClaim,
        String publicAlg,
        String audienceClaim,
        String audience,
        List<String> adminRoles,
        String jwksHostsAllowlist
) {}