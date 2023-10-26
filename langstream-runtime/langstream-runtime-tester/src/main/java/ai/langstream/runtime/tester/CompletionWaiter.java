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
package ai.langstream.runtime.tester;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CompletionWaiter {

    private final CountDownLatch counter;
    private AtomicReference<Throwable> firstError = new AtomicReference<>();

    public CompletionWaiter(List<CompletableFuture<?>> futures) {
        this.counter = new CountDownLatch(futures.size());
        for (CompletableFuture<?> future : futures) {
            future.whenComplete((r, e) -> taskCompleted(e));
        }
    }

    private void taskCompleted(Throwable error) {
        if (error == null) {
            counter.countDown();
        } else {
            firstError.compareAndSet(null, error);
            log.error("Some task failed. Exiting", error);
            long count = counter.getCount();
            for (long i = 0; i < count; i++) {
                counter.countDown();
            }
        }
    }

    public boolean awaitCompletion(int time, TimeUnit unit) throws Throwable {
        boolean done = counter.await(time, unit);
        if (firstError.get() != null) {
            throw firstError.get();
        }
        return done;
    }
}
