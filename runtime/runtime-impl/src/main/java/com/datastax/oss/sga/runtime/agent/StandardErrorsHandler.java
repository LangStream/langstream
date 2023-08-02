/**
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
package com.datastax.oss.sga.runtime.agent;

import com.datastax.oss.sga.api.runner.code.Record;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
class StandardErrorsHandler implements ErrorsHandler {

    private final int retries;
    private final String onFailureAction;

    private AtomicInteger failures = new AtomicInteger(0);

    public static final String FAIL = "fail";
    public static final String SKIP = "skip";

    public StandardErrorsHandler(Map<String, Object> configuration) {
        if (configuration == null) {
            configuration = Map.of();
        }
        this.retries = Integer.parseInt(configuration.getOrDefault("retries", "0").toString());
        this.onFailureAction = configuration.getOrDefault("onFailure", FAIL).toString();
    }

    @Override
    public ErrorsProcessingOutcome handleErrors(Record sourceRecord, Throwable error) {
        // no stacktrace here, it's too verbose
        int currentFailures = failures.incrementAndGet();
        log.info("Handling error {} for source record {}, errors count {} (max retries {})", error + "",
                sourceRecord, currentFailures, retries);
        if (currentFailures >= retries) {
            switch (onFailureAction) {
                case SKIP:
                    return ErrorsProcessingOutcome.SKIP;
                case FAIL:
                default:
                    return ErrorsProcessingOutcome.FAIL;
            }
        } else {
            return ErrorsProcessingOutcome.RETRY;
        }
    }
}
