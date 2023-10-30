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

    @JsonProperty("service-options")
    private ServiceOptions serviceOptions;

    @JsonProperty("events-topic")
    private String eventsTopic;

    public enum GatewayType {
        produce,
        consume,
        chat,
        service
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

        public KeyValueComparison {
            if (value == null && valueFromAuthentication == null && valueFromParameters == null) {
                throw new IllegalArgumentException(
                        "Must specify one of value, value-from-parameters, or value-from-authentication");
            }
            if (value != null && valueFromParameters != null) {
                throw new IllegalArgumentException(
                        "Cannot specify both value and value-from-parameters");
            }

            if (value != null && valueFromAuthentication != null) {
                throw new IllegalArgumentException(
                        "Cannot specify both value and value-from-authentication");
            }

            if (valueFromAuthentication != null && valueFromParameters != null) {
                throw new IllegalArgumentException(
                        "Cannot specify both value-from-parameters and value-from-authentication");
            }
            if (key == null) {
                if (value != null) {
                    key = value;
                } else if (valueFromParameters != null) {
                    key = valueFromParameters;
                } else {
                    key = valueFromAuthentication;
                }
                if (key == null) {
                    throw new IllegalArgumentException("Not able to compute key: " + this);
                }
            }
        }

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

    public record ProduceOptions(List<KeyValueComparison> headers) {}

    public record ConsumeOptions(ConsumeOptionsFilters filters) {}

    public record ConsumeOptionsFilters(List<KeyValueComparison> headers) {}

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ChatOptions {

        @JsonProperty("questions-topic")
        private String questionsTopic;

        @JsonProperty("answers-topic")
        private String answersTopic;

        List<KeyValueComparison> headers;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ServiceOptions {

        @JsonProperty("agent-id")
        private String agentId;

        @JsonProperty("input-topic")
        private String inputTopic;

        @JsonProperty("output-topic")
        private String outputTopic;

        List<KeyValueComparison> headers;
    }
}
