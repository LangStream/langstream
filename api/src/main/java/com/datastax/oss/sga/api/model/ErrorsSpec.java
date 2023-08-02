/**
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
