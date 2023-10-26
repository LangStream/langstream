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
package tests;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.langstream.runtime.tester.CompletionWaiter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class CompletionWaiterTest {

    static ScheduledExecutorService timer;

    @BeforeAll
    public static void beforeAll() {
        timer = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterAll
    public static void afterAll() {
        timer.shutdown();
    }

    @Test
    public void testTimeout() throws Throwable {
        List<CompletableFuture<?>> futures = new ArrayList<>();
        futures.add(new CompletableFuture<>());
        futures.add(new CompletableFuture<>());
        CompletionWaiter waiter = new CompletionWaiter(futures);
        assertFalse(waiter.awaitCompletion(5, java.util.concurrent.TimeUnit.SECONDS));
    }

    @Test
    public void testAllCompleted() throws Throwable {
        List<CompletableFuture<?>> futures = new ArrayList<>();
        futures.add(CompletableFuture.completedFuture(null));
        futures.add(CompletableFuture.completedFuture(null));
        CompletionWaiter waiter = new CompletionWaiter(futures);
        assertTrue(waiter.awaitCompletion(5, java.util.concurrent.TimeUnit.SECONDS));
    }

    @Test
    public void testAllCompletedDeferred() throws Throwable {
        List<CompletableFuture<?>> futures = Collections.synchronizedList(new ArrayList<>());
        futures.add(new CompletableFuture<>());
        futures.add(new CompletableFuture<>());
        CompletionWaiter waiter = new CompletionWaiter(futures);
        timer.schedule(
                () -> {
                    futures.get(0).complete(null);
                },
                2,
                TimeUnit.SECONDS);
        timer.schedule(
                () -> {
                    futures.get(1).complete(null);
                },
                2,
                TimeUnit.SECONDS);
        assertTrue(waiter.awaitCompletion(10, java.util.concurrent.TimeUnit.SECONDS));
    }

    @Test
    public void testOnErrorAndOneNotCompleted() throws Throwable {
        List<CompletableFuture<?>> futures = new ArrayList<>();
        futures.add(CompletableFuture.failedFuture(new IllegalStateException()));
        futures.add(new CompletableFuture<>());
        CompletionWaiter waiter = new CompletionWaiter(futures);
        assertThrows(
                IllegalStateException.class,
                () -> {
                    waiter.awaitCompletion(
                            Integer.MAX_VALUE, java.util.concurrent.TimeUnit.SECONDS);
                });
    }

    @Test
    public void testOneErrorDeferred() throws Throwable {
        List<CompletableFuture<?>> futures = Collections.synchronizedList(new ArrayList<>());
        futures.add(new CompletableFuture<>());
        futures.add(new CompletableFuture<>());
        CompletionWaiter waiter = new CompletionWaiter(futures);
        timer.schedule(
                () -> {
                    futures.get(0).completeExceptionally(new IllegalStateException());
                },
                2,
                TimeUnit.SECONDS);
        timer.schedule(
                () -> {
                    futures.get(1).complete(null);
                },
                2,
                TimeUnit.SECONDS);
        assertThrows(
                IllegalStateException.class,
                () -> {
                    waiter.awaitCompletion(
                            Integer.MAX_VALUE, java.util.concurrent.TimeUnit.SECONDS);
                });
    }
}
