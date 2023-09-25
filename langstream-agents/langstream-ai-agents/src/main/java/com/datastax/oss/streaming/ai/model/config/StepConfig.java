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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", visible = true)
@JsonSubTypes(
        value = {
            @JsonSubTypes.Type(value = DropFieldsConfig.class, name = "drop-fields"),
            @JsonSubTypes.Type(value = UnwrapKeyValueConfig.class, name = "unwrap-key-value"),
            @JsonSubTypes.Type(value = MergeKeyValueConfig.class, name = "merge-key-value"),
            @JsonSubTypes.Type(value = CastConfig.class, name = "cast"),
            @JsonSubTypes.Type(value = DropConfig.class, name = "drop"),
            @JsonSubTypes.Type(value = FlattenConfig.class, name = "flatten"),
            @JsonSubTypes.Type(value = ComputeConfig.class, name = "compute"),
            @JsonSubTypes.Type(
                    value = ComputeAIEmbeddingsConfig.class,
                    name = "compute-ai-embeddings"),
            @JsonSubTypes.Type(value = ChatCompletionsConfig.class, name = "ai-chat-completions"),
            @JsonSubTypes.Type(value = TextCompletionsConfig.class, name = "ai-text-completions"),
            @JsonSubTypes.Type(value = QueryConfig.class, name = "query")
        })
@Getter
public abstract class StepConfig {
    @JsonProperty(required = true)
    private String type;

    @JsonProperty private String when;
}
