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
package ai.langstream.deployer.k8s.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class KeyedLockHandler {

    public interface LockHolder extends AutoCloseable {
        void release();

        default void close() {
            release();
        }
    }

    private final Map<String, Lock> mapStringLock = new ConcurrentHashMap<>();

    public LockHolder lock(String key) {
        Lock lock = mapStringLock.computeIfAbsent(key, k -> new ReentrantLock());
        lock.lock();
        return new LockHolder() {
            @Override
            public void release() {
                lock.unlock();
            }
        };
    }
}
