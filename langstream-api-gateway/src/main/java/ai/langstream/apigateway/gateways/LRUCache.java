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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class LRUCache<K, V extends LRUCache.CachedObject<? super V>>
        implements AutoCloseable {

    public abstract static class CachedObject<T extends AutoCloseable> implements AutoCloseable {
        T object;
        private volatile int referenceCount;
        private volatile boolean cached = true;

        public CachedObject(T object) {
            this.object = object;
        }

        boolean acquire() {
            synchronized (this) {
                if (object == null) {
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
                    try {
                        object.close();
                    } catch (Exception e) {
                        log.warn("Error closing object", e);
                    }
                    object = null;
                }
            }
        }

        @Override
        public void close() {
            synchronized (this) {
                referenceCount--;
                if (referenceCount == 0 && !cached) {
                    try {
                        object.close();
                    } catch (Exception e) {
                        log.warn("Error closing object", e);
                    }
                    object = null;
                }
            }
        }
    }

    @Getter private final Cache<K, V> cache;

    public LRUCache(int size) {
        this.cache =
                CacheBuilder.newBuilder()
                        .maximumSize(size)
                        .expireAfterWrite(10, TimeUnit.MINUTES)
                        .expireAfterAccess(10, TimeUnit.MINUTES)
                        .removalListener(
                                (RemovalNotification<K, V> notification) -> {
                                    V resource = notification.getValue();
                                    if (resource != null) {
                                        resource.removedFromCache();
                                    }
                                })
                        .recordStats()
                        .build();
    }

    public V getOrCreate(K key, Supplier<V> objectSupplier) {
        try {
            final V sharedObject =
                    cache.get(
                            key,
                            () -> {
                                final var result = objectSupplier.get();
                                log.debug("Created shared object {}", key);
                                log.info("Created shared object {}", key);
                                return result;
                            });
            if (!sharedObject.acquire()) {
                log.warn("Not able to acquire object {}, retry", key);
                return getOrCreate(key, objectSupplier);
            }
            return sharedObject;
        } catch (ExecutionException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() {
        log.info("Closing object cache for {}", this.getClass().getSimpleName());
        cache.invalidateAll();
    }
}
