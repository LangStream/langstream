package com.datastax.oss.sga.webservice.security.infrastructure.primary;

import com.datastax.oss.sga.webservice.config.AuthTokenProperties;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.GenericFilterBean;

public class TokenAuthFilter extends GenericFilterBean {

    private static final Logger log = LoggerFactory.getLogger(TokenAuthFilter.class);
    private static final String HTTP_HEADER_VALUE_PREFIX = "Bearer ";
    private final AuthenticationProviderToken authenticationProvider;
    private final AuthTokenProperties tokenProperties;

    @SneakyThrows
    public TokenAuthFilter(AuthTokenProperties tokenProperties) {
        this.tokenProperties = tokenProperties;
        this.authenticationProvider = new AuthenticationProviderToken(tokenProperties);
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain chain)
            throws IOException, ServletException {
        try {
            String httpHeaderValue = ((HttpServletRequest) servletRequest).getHeader(HttpHeaders.AUTHORIZATION);
            final String token;
            if (httpHeaderValue == null || httpHeaderValue.length() <= HTTP_HEADER_VALUE_PREFIX.length()) {
                throw new AuthenticationProviderToken.AuthenticationException("Missing token");
            } else {
                token = httpHeaderValue.substring(HTTP_HEADER_VALUE_PREFIX.length());
            }

            if (log.isDebugEnabled()) {
                log.debug("Authenticating user with token: {}", token);
            }
            String role = authenticationProvider.authenticate(token);
            if (log.isDebugEnabled()) {
                log.debug("Authenticated user: {} with role: {}", token, role);
            }

            List<GrantedAuthority> authorities = null;
            if (tokenProperties.adminRoles() != null && tokenProperties.adminRoles().contains(role)) {
                authorities = Collections.singletonList(new SimpleGrantedAuthority("ROLE_ADMIN"));
            }

            UsernamePasswordAuthenticationToken authentication = new UsernamePasswordAuthenticationToken(role, token, authorities);
            SecurityContextHolder.getContext().setAuthentication(authentication);
        } catch (AuthenticationProviderToken.AuthenticationException e) {
            log.debug(e.getMessage());
            SecurityContextHolder.getContext().setAuthentication(UsernamePasswordAuthenticationToken.unauthenticated(null, null));
        }
        chain.doFilter(servletRequest, servletResponse);
    }
}