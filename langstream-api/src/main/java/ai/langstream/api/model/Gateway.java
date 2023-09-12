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
package ai.langstream.api.model;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public final class Gateway {

    private String id;
    private GatewayType type;
    private String topic;
    private Authentication authentication;
    private List<String> parameters;
    @JsonAlias({"produce-options"})
    private ProduceOptions produceOptions;
    @JsonAlias({"consume-options"})
    private ConsumeOptions consumeOptions;
    @JsonProperty("chat-options")
    private ChatOptions chatOptions;
    @JsonProperty("events-topic")
    private String eventsTopic;

    public enum GatewayType {
        produce,
        consume
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Authentication {
        private String provider;
        private Map<String, Object> configuration;

        @JsonProperty("allow-test-mode")
        private boolean allowTestMode = true;
    }

    public record KeyValueComparison(
            String key,
            String value,
            @JsonAlias({"value-from-parameters"}) String valueFromParameters,
            @JsonAlias({"value-from-authentication"}) String valueFromAuthentication) {
        public static KeyValueComparison value(String key, String value) {
            return new KeyValueComparison(key, value, null, null);
        }

        public static KeyValueComparison valueFromParameters(
                String key, String valueFromParameters) {
            return new KeyValueComparison(key, null, valueFromParameters, null);
        }

        public static KeyValueComparison valueFromAuthentication(
                String key, String valueFromAuthentication) {
            return new KeyValueComparison(key, null, null, valueFromAuthentication);
        }
    }

    public record ProduceOptions(List<KeyValueComparison> headers) {
    }

    public record ConsumeOptions(ConsumeOptionsFilters filters) {
    }

    public record ConsumeOptionsFilters(List<KeyValueComparison> headers) {
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ChatOptions {


        @JsonProperty("questions-topic")
        private String questionsTopic;
        @JsonProperty("answers-topic")
        private String answersTopic;

        @JsonProperty("session-parameter")
        private String sessionParameter;
        @JsonProperty("user-parameter")
        private String userParameter;
    }
}
