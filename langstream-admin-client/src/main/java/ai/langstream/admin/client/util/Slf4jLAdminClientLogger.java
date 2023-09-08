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
package ai.langstream.admin.client.util;

import ai.langstream.admin.client.AdminClientLogger;
import org.slf4j.Logger;

public class Slf4jLAdminClientLogger implements AdminClientLogger {

    private final org.slf4j.Logger log;

    public Slf4jLAdminClientLogger(Logger log) {
        this.log = log;
    }

    @Override
    public void log(Object message) {
        if (message != null) {
            log.info(message.toString());
        }
    }

    @Override
    public void error(Object message) {
        if (message != null) {
            log.error(message.toString());
        }
    }

    @Override
    public boolean isDebugEnabled() {
        return log.isDebugEnabled();
    }

    @Override
    public void debug(Object message) {
        if (message != null) {
            log.debug(message.toString());
        }
    }
}
