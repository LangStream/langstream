/*
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
package com.datastax.oss.streaming.ai.model.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.Getter;

@Getter
public class QueryConfig extends StepConfig {
    @JsonProperty(value = "query", required = true)
    private String query;

    @JsonProperty(value = "loop-over")
    private String loopOver;

    @JsonProperty(value = "fields")
    private List<String> fields;

    @JsonProperty(value = "output-field", required = true)
    private String outputField;

    @JsonProperty(value = "only-first")
    private boolean onlyFirst;

    @JsonProperty(value = "mode")
    private String mode = "query";

    @JsonProperty("generated-keys")
    private List<String> generatedKeys;

    @JsonProperty("bucket-name")
    private String bucketName;

    @JsonProperty("scope-name")
    private String scopeName;

    @JsonProperty("collection-name")
    private String collectionName;
}
