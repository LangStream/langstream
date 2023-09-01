package ai.langstream.admin.client.http;

import java.io.IOException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GenericRetryExecution implements Retry {
    private final RetryPolicy policy;
    private int currentAttempts = 0;

    public GenericRetryExecution(RetryPolicy policy) {
        this.policy = policy;
    }

    @Override
    public Optional<Long> shouldRetryAfter(Exception exception, HttpRequest request, HttpResponse<?> response) {
        final boolean retryable = isRetryable(exception, response);
        if (!retryable) {
            return Optional.empty();
        }
        currentAttempts++;
        final long delay = policy.delay(currentAttempts);
        log.info("Retrying request {} after {} ms, status code :{}, err: {}", request.uri(), delay, response == null ? null : response.statusCode(), exception == null ? null : exception.getClass().getName() + " " + exception.getMessage());
        return Optional.of(delay);
    }

    boolean isRetryable(Exception exception, HttpResponse<?> response) {
        if (exception != null && !isExceptionRetryable(exception)) {
            return false;
        }
        if (response != null && !isResponseCodeRetryable(response)) {
            return false;
        }

        if (currentAttempts >= policy.maxAttempts()) {
            return false;
        }
        return true;
    }

    private boolean isResponseCodeRetryable(HttpResponse<?> response) {
        final int code = response.statusCode();
        if (code >= 500) {
            return true;
        }
        return false;

    }

    private boolean isExceptionRetryable(Exception exception) {
        if (exception instanceof IOException) {
            return true;
        }
        return false;
    }
}
