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
package com.datastax.oss.sga.api.model;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
public class ApplicationStatus {

    @Data
    public static class AgentStatus {
        private AgentLifecycleStatus status;
        private Map<String, AgentWorkerStatus> workers;

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AgentWorkerStatus {

        private Status status;
        private String reason;
        private Map<String, Object> info;

        public static final AgentWorkerStatus INITIALIZING =
                new AgentWorkerStatus(Status.INITIALIZING, null, null);

        public static final AgentWorkerStatus RUNNING =
                new AgentWorkerStatus(Status.RUNNING, null, null);

        public static final AgentWorkerStatus error(String reason) {
            return new AgentWorkerStatus(Status.ERROR, reason, null);
        }

        public AgentWorkerStatus withInfo(Map<String, Object> info) {
            return new AgentWorkerStatus(this.status, this.reason, info);
        }


        public enum Status {
            INITIALIZING,
            RUNNING,
            ERROR;
        }

    }

    private ApplicationLifecycleStatus status;
    private Map<String, AgentStatus> agents;
}
