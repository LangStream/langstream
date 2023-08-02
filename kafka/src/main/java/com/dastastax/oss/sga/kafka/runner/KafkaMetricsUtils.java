package com.dastastax.oss.sga.kafka.runner;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class KafkaMetricsUtils {

    private record MetricKey (String group, Map<String, String> tags) {
        public String asKey() {
            if (tags.isEmpty()) {
                return group;
            } else {
                return group + tags
                        .entrySet()
                        .stream()
                        .map(e -> e.getKey() + "=" + e.getValue())
                        .collect(Collectors.joining(",", "[", "]"));
            }
        }
    }


    public static Map<String, Object> metricsToMap(Map<MetricName, ? extends Metric> metrics) {
        Map<String, Object> groupsMetrics = new HashMap<>();
        Set<MetricKey> groups = metrics.keySet()
                .stream()
                .map(m -> new MetricKey(m.group(), m.tags()))
                .collect(Collectors.toSet());
        for (MetricKey group : groups) {
            Map<String, Object> groupMetrics = new HashMap<>();
            metrics.forEach( (metric, value) -> {
                MetricKey metricKey = new MetricKey(metric.group(), metric.tags());
                if (metricKey.equals(group)) {
                    groupMetrics.put(metric.name(), value.metricValue());
                }
            });
            groupsMetrics.put(group.asKey(), groupMetrics);
        }
        return groupsMetrics;
    }
}
