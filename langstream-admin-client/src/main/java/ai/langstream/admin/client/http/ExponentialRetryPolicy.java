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

public class ExponentialRetryPolicy implements RetryPolicy {

    protected static final int DEFAULT_INITIAL_INTERVAL = 2000;
    protected static final double DEFAULT_INTERVAL_MULTIPLIER = 1.5d;
    protected static final int DEFAULT_MAX_ATTEMPTS = 5;
    private final int maxAttempts;
    private final long[] intervals;

    public ExponentialRetryPolicy(
            int maxAttempts, long initialInterval, double intervalMultiplier) {
        this.maxAttempts = maxAttempts;
        this.intervals = new long[maxAttempts];
        this.intervals[0] = initialInterval;
        for (int i = 1; i < maxAttempts; i++) {
            intervals[i] = (long) (intervals[i - 1] * intervalMultiplier);
        }
    }

    public ExponentialRetryPolicy(int maxAttempts, long initialInterval) {
        this(maxAttempts, initialInterval, DEFAULT_INTERVAL_MULTIPLIER);
    }

    public ExponentialRetryPolicy(int maxAttempts) {
        this(maxAttempts, DEFAULT_INITIAL_INTERVAL, DEFAULT_INTERVAL_MULTIPLIER);
    }

    public ExponentialRetryPolicy() {
        this(DEFAULT_MAX_ATTEMPTS, DEFAULT_INITIAL_INTERVAL, DEFAULT_INTERVAL_MULTIPLIER);
    }

    @Override
    public int maxAttempts() {
        return maxAttempts;
    }

    @Override
    public long delay(int attempt) {
        if (attempt > intervals.length) {
            throw new IllegalArgumentException();
        }
        return intervals[attempt - 1];
    }

    long[] getIntervals() {
        return intervals;
    }
}
