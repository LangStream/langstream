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
import net.bytebuddy.implementation.bytecode.Throw;

import java.util.List;

public interface ErrorsHandler {

    enum ErrorsProcessingOutcome {
        SKIP,
        RETRY,
        FAIL
    }

    ErrorsProcessingOutcome handleErrors(Record sourceRecord, Throwable error);

    boolean failProcessingOnPermanentErrors();
}
