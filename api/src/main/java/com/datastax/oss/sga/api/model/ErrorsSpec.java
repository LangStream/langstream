package com.datastax.oss.sga.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ErrorsSpec {

    public static final String FAIL = "fail";
    public static final String SKIP = "skip";
    public static final String DEAD_LETTER = "dead-letter";

    @JsonProperty("on-failure")
    private String onFailure;

    private Integer retries;

    public static final ErrorsSpec DEFAULT = new ErrorsSpec(FAIL, 0);

    public ErrorsSpec withDefaultsFrom(ErrorsSpec higherLevel){
        if (higherLevel == null) {
            return this;
        }
        String newOnFailure = onFailure == null ? higherLevel.getOnFailure() : onFailure;
        Integer newRetries = retries == null ? higherLevel.getRetries() : retries;
        return new ErrorsSpec(newOnFailure, newRetries);
    }
}
