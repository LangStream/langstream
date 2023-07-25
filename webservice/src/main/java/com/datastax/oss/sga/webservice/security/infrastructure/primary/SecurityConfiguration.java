package com.datastax.oss.sga.webservice.security.infrastructure.primary;

import com.datastax.oss.sga.webservice.config.AuthTokenProperties;
import lombok.AllArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityCustomizer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.web.filter.CorsFilter;

@Configuration
@EnableWebSecurity
@EnableMethodSecurity(securedEnabled = true)
@AllArgsConstructor
public class SecurityConfiguration {

  private final CorsFilter corsFilter;
  private final AuthTokenProperties properties;

  @Bean
  @ConditionalOnProperty(name = "application.security.enabled", havingValue = "false", matchIfMissing = true)
  public WebSecurityCustomizer webSecurityCustomizer() {
    return web -> web.ignoring().anyRequest();
  }

  @Bean
  @ConditionalOnProperty(name = "application.security.enabled", havingValue = "true")
  public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
    // @formatter:off
    return http
        .csrf().disable()
        .addFilterBefore(corsFilter, UsernamePasswordAuthenticationFilter.class)
        .addFilterBefore(new TokenAuthFilter(properties), UsernamePasswordAuthenticationFilter.class)
        .formLogin().disable()
        .httpBasic().disable()
        .sessionManagement()
          .sessionCreationPolicy(SessionCreationPolicy.STATELESS)
          .and()
        .authorizeHttpRequests()
          .requestMatchers(HttpMethod.OPTIONS, "/**").permitAll()
          .requestMatchers("/swagger-ui/**").permitAll()
          .requestMatchers("/swagger-ui.html").permitAll()
          .requestMatchers("/v3/api-docs/**").permitAll()
          .requestMatchers("/api/tenants/**").hasAuthority("ROLE_ADMIN")
          .requestMatchers("/api/**").authenticated()
          .requestMatchers("/management/health").permitAll()
          .requestMatchers("/management/health/**").permitAll()
          .requestMatchers("/management/info").permitAll()
          .requestMatchers("/management/prometheus").permitAll()
          .requestMatchers("/management/**").authenticated()
          .anyRequest().authenticated()
          .and()
      .build();
    // @formatter:on
  }
}
