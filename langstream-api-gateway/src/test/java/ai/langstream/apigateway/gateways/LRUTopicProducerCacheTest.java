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

import static org.junit.jupiter.api.Assertions.*;

import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.runner.topics.TopicProducer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

class LRUTopicProducerCacheTest {

    private static class NoopTopicProducer implements TopicProducer {
        private volatile boolean closed;

        @Override
        public CompletableFuture<?> write(Record record) {
            synchronized (this) {
                if (closed) {
                    throw new RuntimeException("Closed");
                }
                return CompletableFuture.completedFuture(null);
            }
        }

        @Override
        public long getTotalIn() {
            return 0;
        }

        @Override
        public void close() {
            synchronized (this) {
                closed = true;
            }
        }
    }

    @Test
    void testGetOrCreate() {
        final LRUTopicProducerCache cache = new LRUTopicProducerCache(2);
        AtomicInteger initCounter = new AtomicInteger();
        AtomicInteger closeCounter = new AtomicInteger();
        final Supplier<TopicProducer> topicProducerSupplier =
                () -> {
                    initCounter.incrementAndGet();
                    return new NoopTopicProducer() {
                        @Override
                        public void close() {
                            closeCounter.incrementAndGet();
                        }
                    };
                };
        final TopicProducer first =
                cache.getOrCreate(
                        newKey("tenant", "application", "gatewayId"), topicProducerSupplier);
        cache.getOrCreate(newKey("tenant", "application", "gatewayId"), topicProducerSupplier);
        cache.getOrCreate(newKey("tenant", "application", "gatewayId"), topicProducerSupplier);

        final TopicProducer second =
                cache.getOrCreate(
                        newKey("tenant", "application", "gatewayId2"), topicProducerSupplier);

        assertEquals(2, initCounter.get());
        assertEquals(0, closeCounter.get());
        cache.getOrCreate(newKey("tenant", "application", "gatewayId3"), topicProducerSupplier);
        assertEquals(3, initCounter.get());
        assertEquals(0, closeCounter.get());
        first.close();
        assertEquals(0, closeCounter.get());
        first.close();
        assertEquals(0, closeCounter.get());
        first.close();
        assertEquals(1, closeCounter.get());

        assertEquals(2, cache.getCache().size());
        cache.getOrCreate(newKey("tenant", "application", "gatewayId"), topicProducerSupplier);
        assertEquals(4, initCounter.get());
        assertEquals(2, cache.getCache().size());
        assertEquals(1, closeCounter.get());
        second.close();
        assertEquals(2, closeCounter.get());
    }

    @Test
    void testConcurrency() {
        final LRUTopicProducerCache cache = new LRUTopicProducerCache(2);
        AtomicInteger initCounter = new AtomicInteger();
        AtomicInteger closeCounter = new AtomicInteger();
        final Supplier<TopicProducer> topicProducerSupplier =
                () -> {
                    initCounter.incrementAndGet();
                    return new NoopTopicProducer() {
                        @Override
                        public void close() {
                            closeCounter.incrementAndGet();
                        }
                    };
                };
        final int nThreads = 50;
        final ExecutorService pool = Executors.newFixedThreadPool(nThreads);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < nThreads; i++) {
            futures.add(
                    CompletableFuture.runAsync(
                            () -> {
                                for (int j = 0; j < 100; j++) {
                                    final TopicProducer prod0 =
                                            cache.getOrCreate(
                                                    newKey("tenant", "application", "gatewayId"),
                                                    topicProducerSupplier);
                                    prod0.write(SimpleRecord.of("key", "value"));
                                    prod0.close();
                                    final TopicProducer prod1 =
                                            cache.getOrCreate(
                                                    newKey("tenant", "application", "gatewayId1"),
                                                    topicProducerSupplier);
                                    prod1.write(SimpleRecord.of("key", "value"));
                                    prod1.close();
                                    final TopicProducer prod2 =
                                            cache.getOrCreate(
                                                    newKey("tenant", "application", "gatewayId2"),
                                                    topicProducerSupplier);
                                    prod2.write(SimpleRecord.of("key", "value"));
                                    prod2.close();
                                }
                            },
                            pool));
        }
        futures.forEach(CompletableFuture::join);
        assertEquals(initCounter.get() - 2, closeCounter.get());
    }

    private static TopicProducerCache.Key newKey(
            String tenant, String application, String gateway) {
        return new TopicProducerCache.Key(tenant, application, gateway, null, null);
    }
}
