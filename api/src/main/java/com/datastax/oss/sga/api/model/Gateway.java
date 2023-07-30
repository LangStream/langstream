package com.datastax.oss.sga.api.model;

import java.util.List;
import java.util.Map;
import java.util.logging.Filter;

public record Gateway(String id, GatewayType type, String topic,
                      Authentication authentication,
                      List<String> parameters,
                      ProduceOptions produceOptions,
                      ConsumeOptions consumeOptions) {
    public enum GatewayType {
        produce,
        consume;
    }

    public Gateway(String id, GatewayType type, String topic, List<String> parameters,
                   ProduceOptions produceOptions, ConsumeOptions consumeOptions) {
        this(id, type, topic, null, parameters, produceOptions, consumeOptions);
    }

    public record Authentication(String provider, Map<String, Object> configuration) {}

    public record KeyValueComparison(String key, String value, String valueFromParameters, String valueFromAuthentication) {
        public static KeyValueComparison value(String key, String value) {
            return new KeyValueComparison(key, value, null, null);
        }
        public static KeyValueComparison valueFromParameters(String key, String valueFromParameters) {
            return new KeyValueComparison(key, null, valueFromParameters, null);
        }
        public static KeyValueComparison valueFromAuthentication(String key, String valueFromAuthentication) {
            return new KeyValueComparison(key, null, null, valueFromAuthentication);
        }


    }
    public record ProduceOptions(List<KeyValueComparison> headers) {}

    public record ConsumeOptions(ConsumeOptionsFilters filters) {}
    public record ConsumeOptionsFilters(List<KeyValueComparison> headers) {}


}
