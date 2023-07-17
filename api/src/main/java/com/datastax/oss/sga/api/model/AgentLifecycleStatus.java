package com.datastax.oss.sga.api.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AgentLifecycleStatus {

    public static final AgentLifecycleStatus CREATED =
            new AgentLifecycleStatus(Status.CREATED, null);
    public static final AgentLifecycleStatus DEPLOYING =
            new AgentLifecycleStatus(Status.DEPLOYING, null);
    public static final AgentLifecycleStatus DEPLOYED =
            new AgentLifecycleStatus(Status.DEPLOYED, null);

    public static final AgentLifecycleStatus error(String reason) {
        return new AgentLifecycleStatus(Status.ERROR, reason);
    }

    private Status status;
    private String reason;

    public enum Status {
        CREATED,
        DEPLOYING,
        DEPLOYED,
        ERROR;
    }
}
