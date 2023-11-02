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

public class PrometheusMetricsReporter implements MetricsReporter {

    private final String prefix;

    public PrometheusMetricsReporter(String prefix) {
        this.prefix = prefix;
    }

    public PrometheusMetricsReporter() {
        this("");
    }

    @Override
    public MetricsReporter withPrefix(String prefix) {
        String newPrefix = this.prefix.isEmpty() ? prefix : this.prefix + "_" + prefix;
        return new PrometheusMetricsReporter(newPrefix);
    }

    @Override
    public Counter counter(String name) {
        String finalName = this.prefix.isEmpty() ? prefix : this.prefix + "_" + name;
        io.prometheus.client.Counter counter =
                io.prometheus.client.Counter.build().name(finalName).register();
        return new Counter() {
            @Override
            public void count(int value) {
                counter.inc(value);
            }
        };
    }
}
