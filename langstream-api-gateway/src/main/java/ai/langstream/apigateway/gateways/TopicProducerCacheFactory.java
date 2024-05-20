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
package ai.langstream.apigateway.gateways;

import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.apigateway.MetricsNames;
import ai.langstream.apigateway.config.TopicProperties;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.cache.GuavaCacheMetrics;
import java.util.function.Supplier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TopicProducerCacheFactory {

    @Bean
    public TopicProducerCache topicProducerCache(TopicProperties topicProperties) {
        if (topicProperties.isProducersCacheEnabled()) {
            final LRUTopicProducerCache cache =
                    new LRUTopicProducerCache(topicProperties.getProducersCacheSize());
            GuavaCacheMetrics.monitor(
                    Metrics.globalRegistry, cache.getCache(), MetricsNames.TOPIC_PRODUCER_CACHE);
            return cache;
        } else {
            return new TopicProducerCache() {
                @Override
                public TopicProducer getOrCreate(
                        Key key, Supplier<TopicProducer> topicProducerSupplier) {
                    return topicProducerSupplier.get();
                }

                @Override
                public void close() {}
            };
        }
    }
}
