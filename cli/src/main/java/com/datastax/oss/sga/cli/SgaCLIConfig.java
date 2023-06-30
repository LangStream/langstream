package com.datastax.oss.sga.cli;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

@Getter
public class SgaCLIConfig {

    @JsonProperty(required = true)
    private String webServiceUrl;

}
