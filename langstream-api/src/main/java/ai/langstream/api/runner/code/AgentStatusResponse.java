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
package ai.langstream.api.runner.code;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public final class AgentStatusResponse {

    @JsonProperty("agent-id")
    private String agentId;

    @JsonProperty("agent-type")
    private String agentType;

    @JsonProperty("component-type")
    private String componentType;

    @JsonProperty("info")
    private Map<String, Object> info;

    @JsonProperty("metrics")
    private Metrics metrics;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static final class Metrics {
        @JsonProperty("total-in")
        private Long totalIn;

        @JsonProperty("total-out")
        private Long totalOut;

        @JsonProperty("started-at")
        private Long startedAt;

        @JsonProperty("last-processed-at")
        private Long lastProcessedAt;
    }
}
