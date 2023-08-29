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
package ai.langstream.runtime.agent;

import static ai.langstream.api.model.ErrorsSpec.DEAD_LETTER;
import static ai.langstream.api.model.ErrorsSpec.FAIL;
import static ai.langstream.api.model.ErrorsSpec.SKIP;

import ai.langstream.api.runner.code.Record;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class StandardErrorsHandler implements ErrorsHandler {

    private final int retries;
    private final String onFailureAction;

    private final AtomicInteger failures = new AtomicInteger(0);

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
        log.info(
                "Handling error {} for source record {}, errors count {} (max retries {})",
                error + "",
                sourceRecord,
                currentFailures,
                retries);
        if (currentFailures >= retries) {
            return switch (onFailureAction) {
                case SKIP -> ErrorsProcessingOutcome.SKIP;
                case FAIL, DEAD_LETTER -> ErrorsProcessingOutcome.FAIL;
                default -> ErrorsProcessingOutcome.FAIL;
            };
        } else {
            return ErrorsProcessingOutcome.RETRY;
        }
    }

    @Override
    public boolean failProcessingOnPermanentErrors() {
        return switch (onFailureAction) {
            case SKIP, DEAD_LETTER -> false;
            case FAIL -> true;
            default -> true;
        };
    }
}
