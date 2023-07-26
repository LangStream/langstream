package com.datastax.oss.sga.api.model;

import java.util.List;
import java.util.Map;
import java.util.logging.Filter;

public record Gateway(String id, String type, String topic, List<String> parameters,
                      ProduceOptions produceOptions,
                      ConsumeOptions consumeOptions) {
    public record ProduceOptions(Map<String, String>headers) {}
    public record ConsumeOptions(List<Filter> filters) {}
    public record ConsumeFilter(String key, String value) {}


}
