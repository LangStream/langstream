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
package ai.langstream.runtime.agent.metrics;

import ai.langstream.api.runner.code.MetricsReporter;
import io.prometheus.client.Counter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PrometheusMetricsReporter implements MetricsReporter {

    private final String agentName;
    private static Map<String, io.prometheus.client.Counter> counters = new ConcurrentHashMap<>();

    public PrometheusMetricsReporter(String agentName) {
        this.agentName = agentName;
    }

    public PrometheusMetricsReporter() {
        this("");
    }

    @Override
    public MetricsReporter withAgentName(String agentName) {
        return new PrometheusMetricsReporter(agentName);
    }

    @Override
    public Counter counter(String name, String help) {
        io.prometheus.client.Counter counter =
                counters.computeIfAbsent(
                        name,
                        k -> {
                            return io.prometheus.client.Counter.build()
                                    .name(sanitizeMetricName(name))
                                    .labelNames("agent_id")
                                    .help(help)
                                    .register();
                        });

        io.prometheus.client.Counter.Child counterWithLabel = counter.labels(agentName);
        return new Counter() {
            @Override
            public void count(long value) {
                counterWithLabel.inc(value);
            }

            @Override
            public long value() {
                return (long) counterWithLabel.get();
            }
        };
    }

    private static String sanitizeMetricName(String metricName) {
        // Define a regular expression pattern to match forbidden characters
        String pattern = "[^a-zA-Z0-9_]+";

        // Replace all forbidden characters with an underscore
        String sanitizedName = metricName.replaceAll(pattern, "_");

        // Make sure the name starts with a letter or underscore
        if (!Character.isLetter(sanitizedName.charAt(0)) && sanitizedName.charAt(0) != '_') {
            sanitizedName = "_" + sanitizedName;
        }

        return sanitizedName;
    }
}
