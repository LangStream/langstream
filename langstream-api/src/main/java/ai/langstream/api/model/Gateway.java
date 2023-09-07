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

public record Gateway(
        String id,
        GatewayType type,
        String topic,
        Authentication authentication,
        List<String> parameters,
        @JsonAlias({"produce-options"}) ProduceOptions produceOptions,
        @JsonAlias({"consume-options"}) ConsumeOptions consumeOptions,
        @JsonProperty("events-topic") String eventsTopic) {
    public enum GatewayType {
        produce,
        consume
    }

    public Gateway(
            String id,
            GatewayType type,
            String topic,
            List<String> parameters,
            ProduceOptions produceOptions,
            ConsumeOptions consumeOptions) {
        this(id, type, topic, null, parameters, produceOptions, consumeOptions, null);
    }

    public Gateway(
            String id,
            GatewayType type,
            String topic,
            Authentication authentication,
            List<String> parameters,
            ProduceOptions produceOptions,
            ConsumeOptions consumeOptions) {
        this(id, type, topic, authentication, parameters, produceOptions, consumeOptions, null);
    }

    public record Authentication(String provider, Map<String, Object> configuration) {}

    public record KeyValueComparison(
            String key, String value, String valueFromParameters, String valueFromAuthentication) {
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
}
