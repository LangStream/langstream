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

        public static final AgentWorkerStatus INITIALIZING =
                new AgentWorkerStatus(Status.INITIALIZING, null);

        public static final AgentWorkerStatus RUNNING =
                new AgentWorkerStatus(Status.RUNNING, null);

        public static final AgentWorkerStatus error(String reason) {
            return new AgentWorkerStatus(Status.ERROR, reason);
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
