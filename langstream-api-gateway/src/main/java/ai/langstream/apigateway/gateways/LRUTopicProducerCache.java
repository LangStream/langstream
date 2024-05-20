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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LRUTopicProducerCache implements TopicProducerCache {

    private static class SharedTopicProducer implements TopicProducer {
        private TopicProducer producer;
        private volatile int referenceCount;
        private volatile boolean cached = true;

        public SharedTopicProducer(TopicProducer producer) {
            this.producer = producer;
        }

        public boolean acquire() {
            synchronized (this) {
                if (producer == null) {
                    return false;
                }
                referenceCount++;
                return true;
            }
        }

        public void removedFromCache() {
            synchronized (this) {
                cached = false;
                if (referenceCount == 0) {
                    producer.close();
                    producer = null;
                }
            }
        }

        @Override
        public void start() {
            producer.start();
        }

        @Override
        public void close() {
            synchronized (this) {
                referenceCount--;
                if (referenceCount == 0 && !cached) {
                    producer.close();
                    producer = null;
                }
            }
        }

        @Override
        public CompletableFuture<?> write(Record record) {
            return producer.write(record);
        }

        @Override
        public Object getNativeProducer() {
            return producer.getNativeProducer();
        }

        @Override
        public Object getInfo() {
            return producer.getInfo();
        }

        @Override
        public long getTotalIn() {
            return 0;
        }
    }

    @Getter private final Cache<Key, SharedTopicProducer> cache;

    public LRUTopicProducerCache(int size) {
        this.cache =
                CacheBuilder.newBuilder()
                        .maximumSize(size)
                        .expireAfterWrite(10, TimeUnit.MINUTES)
                        .expireAfterAccess(10, TimeUnit.MINUTES)
                        .removalListener(
                                (RemovalNotification<Key, SharedTopicProducer> notification) -> {
                                    SharedTopicProducer resource = notification.getValue();
                                    resource.removedFromCache();
                                })
                        .recordStats()
                        .build();
    }

    @Override
    public TopicProducer getOrCreate(
            TopicProducerCache.Key key, Supplier<TopicProducer> topicProducerSupplier) {
        try {
            final SharedTopicProducer sharedTopicProducer =
                    cache.get(
                            key,
                            () -> {
                                final SharedTopicProducer result =
                                        new SharedTopicProducer(topicProducerSupplier.get());
                                log.debug("Created new topic producer {}", key);
                                return result;
                            });
            if (!sharedTopicProducer.acquire()) {
                log.warn("Not able to acquire topic producer {}, retry", key);
                return getOrCreate(key, topicProducerSupplier);
            }
            return sharedTopicProducer;
        } catch (ExecutionException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() {
        cache.invalidateAll();
    }
}
