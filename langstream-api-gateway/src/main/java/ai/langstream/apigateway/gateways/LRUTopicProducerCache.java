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

import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.topics.TopicProducer;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LRUTopicProducerCache
        extends LRUCache<TopicProducerCache.Key, LRUTopicProducerCache.SharedTopicProducer>
        implements TopicProducerCache {

    public static class SharedTopicProducer extends LRUCache.CachedObject<TopicProducer>
            implements TopicProducer {

        public SharedTopicProducer(TopicProducer producer) {
            super(producer);
        }

        @Override
        public void start() {
            object.start();
        }

        @Override
        public CompletableFuture<?> write(Record record) {
            return object.write(record);
        }

        @Override
        public Object getNativeProducer() {
            return object.getNativeProducer();
        }

        @Override
        public Object getInfo() {
            return object.getInfo();
        }

        @Override
        public long getTotalIn() {
            return 0;
        }
    }

    public LRUTopicProducerCache(int size) {
        super(size);
    }

    @Override
    public TopicProducer getOrCreate(
            TopicProducerCache.Key key, Supplier<TopicProducer> topicProducerSupplier) {
        return super.getOrCreate(key, () -> new SharedTopicProducer(topicProducerSupplier.get()));
    }
}
