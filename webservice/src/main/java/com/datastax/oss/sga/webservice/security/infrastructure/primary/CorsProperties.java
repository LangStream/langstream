package com.datastax.oss.sga.webservice.security.infrastructure.primary;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.cors.CorsConfiguration;

@Configuration
public class CorsProperties {

  @Bean
  @ConfigurationProperties(prefix = "application.security.cors", ignoreUnknownFields = false)
  public CorsConfiguration corsConfiguration() {
    return new CorsConfiguration();
  }
}
