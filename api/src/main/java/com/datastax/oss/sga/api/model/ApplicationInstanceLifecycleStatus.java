package com.datastax.oss.sga.api.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ApplicationInstanceLifecycleStatus {

    public static final ApplicationInstanceLifecycleStatus CREATED =
            new ApplicationInstanceLifecycleStatus(Status.CREATED, null);
    public static final ApplicationInstanceLifecycleStatus DEPLOYED =
            new ApplicationInstanceLifecycleStatus(Status.CREATED, null);
    public static final ApplicationInstanceLifecycleStatus DELETING =
            new ApplicationInstanceLifecycleStatus(Status.CREATED, null);

    public static final ApplicationInstanceLifecycleStatus error(String reason) {
        return new ApplicationInstanceLifecycleStatus(Status.ERROR, reason);
    }

    private Status status;
    private String reason;

    public enum Status {
        CREATED,
        DEPLOYED,
        ERROR,
        DELETING;
    }
}
