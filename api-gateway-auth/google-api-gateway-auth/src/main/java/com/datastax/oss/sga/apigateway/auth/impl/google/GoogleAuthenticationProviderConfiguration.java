package com.datastax.oss.sga.apigateway.auth.impl.google;

import com.fasterxml.jackson.annotation.JsonAlias;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class GoogleAuthenticationProviderConfiguration {
    private String clientId;
}
