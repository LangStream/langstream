package ai.langstream.deployer.k8s.controllers;

import io.javaoperatorsdk.operator.processing.retry.GenericRetry;
import io.javaoperatorsdk.operator.processing.retry.GenericRetryExecution;
import io.javaoperatorsdk.operator.processing.retry.Retry;
import io.javaoperatorsdk.operator.processing.retry.RetryExecution;

public class InfiniteRetry implements Retry {
    private static final GenericRetry RETRY = new GenericRetry().withoutMaxAttempts();
    @Override
    public RetryExecution initExecution() {
        return new GenericRetryExecution(RETRY);
    }
}
