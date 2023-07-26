package com.datastax.oss.sga.api.model;

import java.util.List;
import java.util.Map;
import java.util.logging.Filter;

public record Gateway(String id, String type, String topic, List<String> parameters,
                      ProduceOptions produceOptions,
                      ConsumeOptions consumeOptions) {
    public record KeyValueComparison(String key, String value, String valueFromParameters) {}
    public record ProduceOptions(List<KeyValueComparison> headers) {}

    public record ConsumeOptions(ConsumeOptionsFilters filters) {}
    public record ConsumeOptionsFilters(List<KeyValueComparison> headers) {}


}
