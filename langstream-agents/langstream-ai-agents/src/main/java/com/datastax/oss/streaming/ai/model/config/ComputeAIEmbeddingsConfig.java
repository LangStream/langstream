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
import java.util.Map;
import lombok.Getter;

@Getter
public class ComputeAIEmbeddingsConfig extends StepConfig {

    @JsonProperty(required = true)
    private String model;

    @JsonProperty(required = true)
    private String text;

    @JsonProperty(value = "embeddings-field", required = true)
    private String embeddingsFieldName;

    @JsonProperty(value = "loop-over")
    private String loopOver;

    @JsonProperty("batch-size")
    private int batchSize = 10;

    @JsonProperty("concurrency")
    private int concurrency = 4;

    // we disable flushing by default in order to avoid latency spikes
    // you should enable this feature in the case of background processing
    @JsonProperty("flush-interval")
    private int flushInterval = 0;

    @Deprecated
    @JsonProperty(value = "compute-service")
    private String service;

    @JsonProperty Map<String, String> options;

    @JsonProperty Map<String, String> arguments;

    @JsonProperty(value = "model-url")
    String modelUrl;
}
