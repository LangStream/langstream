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
package ai.langstream.kafka.runner;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

public class KafkaMetricsUtils {

    private record MetricKey(String group, Map<String, String> tags) {
        public String asKey() {
            if (tags.isEmpty()) {
                return group;
            } else {
                return group
                        + tags.entrySet().stream()
                                .map(e -> e.getKey() + "=" + e.getValue())
                                .collect(Collectors.joining(",", "[", "]"));
            }
        }
    }

    public static Map<String, Object> metricsToMap(Map<MetricName, ? extends Metric> metrics) {
        Map<String, Object> groupsMetrics = new HashMap<>();
        Set<MetricKey> groups =
                metrics.keySet().stream()
                        .map(m -> new MetricKey(m.group(), m.tags()))
                        .collect(Collectors.toSet());
        for (MetricKey group : groups) {
            Map<String, Object> groupMetrics = new HashMap<>();
            metrics.forEach(
                    (metric, value) -> {
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
