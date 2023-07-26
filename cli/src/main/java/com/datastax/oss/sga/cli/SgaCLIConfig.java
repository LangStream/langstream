package com.datastax.oss.sga.cli;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SgaCLIConfig {

    @JsonProperty(required = true)
    private String webServiceUrl;

    private String apiGatewayUrl;

    private String tenant;

    private String token;

}
