package com.datastax.oss.sga.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ErrorsSpec {

    @JsonProperty("on-failure")
    private String onFailure;

    private Integer retries;

    @JsonProperty("dead-letter-topic")
    private String deadLetterTopic;

    public static final ErrorsSpec DEFAULT = new ErrorsSpec("fail", 0, null);

    public ErrorsSpec withDefaultsFrom(ErrorsSpec higherLevel){
        if (higherLevel == null) {
            return this;
        }
        String newOnFailure = onFailure == null ? higherLevel.getOnFailure() : onFailure;
        Integer newRetries = retries == null ? higherLevel.getRetries() : retries;
        String newDeadLetterTopic = deadLetterTopic == null ? higherLevel.getDeadLetterTopic() : deadLetterTopic;
        return new ErrorsSpec(newOnFailure, newRetries, newDeadLetterTopic);
    }
}
