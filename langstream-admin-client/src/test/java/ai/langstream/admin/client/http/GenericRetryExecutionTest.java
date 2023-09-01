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
package ai.langstream.admin.client.http;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.ConnectException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class GenericRetryExecutionTest {

    @Test
    void testIsRetryable() {
        assertFalse(testIsRetryable(200));
        assertFalse(testIsRetryable(400));
        assertTrue(testIsRetryable(500));
        assertTrue(testIsRetryable(503));
        assertFalse(testIsRetryable(new RuntimeException("")));
        assertTrue(testIsRetryable(new ConnectException("")));
        assertTrue(testIsRetryable(new IOException("")));
    }

    private boolean testIsRetryable(int code) {
        return new GenericRetryExecution(new ExponentialRetryPolicy())
                .isRetryable(null, responseWithCode(code));
    }

    private boolean testIsRetryable(Exception exception) {
        return new GenericRetryExecution(new ExponentialRetryPolicy()).isRetryable(exception, null);
    }

    private static HttpResponse<?> responseWithCode(int code) {
        final HttpResponse mock = mock(HttpResponse.class);
        when(mock.statusCode()).thenReturn(code);
        return mock;
    }

    private static HttpRequest request() {
        final HttpRequest mock = mock(HttpRequest.class);
        return mock;
    }

    @Test
    void testDelays() {
        assertEquals(
                List.of(2000L, 3000L, 4500L, 6750L, 10125L),
                Arrays.stream(new ExponentialRetryPolicy().getIntervals())
                        .boxed()
                        .collect(Collectors.toList()));
    }

    @Test
    void testMaxAttempts() {
        final GenericRetryExecution retry =
                new GenericRetryExecution(new ExponentialRetryPolicy(3));

        assertTrue(retry.shouldRetryAfter(new IOException(), request(), null).isPresent());
        assertTrue(retry.shouldRetryAfter(new IOException(), request(), null).isPresent());
        assertTrue(retry.shouldRetryAfter(new IOException(), request(), null).isPresent());
        assertFalse(retry.shouldRetryAfter(new IOException(), request(), null).isPresent());
    }
}
