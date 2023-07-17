package com.datastax.oss.sga.api.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ApplicationLifecycleStatus {

    public static final ApplicationLifecycleStatus CREATED =
            new ApplicationLifecycleStatus(Status.CREATED, null);
    public static final ApplicationLifecycleStatus DEPLOYING =
            new ApplicationLifecycleStatus(Status.DEPLOYING, null);
    public static final ApplicationLifecycleStatus DEPLOYED =
            new ApplicationLifecycleStatus(Status.DEPLOYED, null);
    public static final ApplicationLifecycleStatus DELETING =
            new ApplicationLifecycleStatus(Status.DELETING, null);

    public static final ApplicationLifecycleStatus error(String reason) {
        return new ApplicationLifecycleStatus(Status.ERROR, reason);
    }

    private Status status;
    private String reason;

    public enum Status {
        CREATED,
        DEPLOYING,
        DEPLOYED,
        ERROR,
        DELETING;
    }
}
