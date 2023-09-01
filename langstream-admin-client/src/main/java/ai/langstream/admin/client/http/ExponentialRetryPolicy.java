package ai.langstream.admin.client.http;


public class ExponentialRetryPolicy implements RetryPolicy {

    protected static final int DEFAULT_INITIAL_INTERVAL = 2000;
    protected static final double DEFAULT_INTERVAL_MULTIPLIER = 1.5d;
    protected static final int DEFAULT_MAX_ATTEMPTS = 5;
    private final int maxAttempts;
    private final long[] intervals;

    public ExponentialRetryPolicy(int maxAttempts, long initialInterval, double intervalMultiplier) {
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
