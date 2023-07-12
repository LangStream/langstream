package com.datastax.oss.sga.api.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class StoredApplication {

    private String name;
    private Application instance;
    private ApplicationInstanceLifecycleStatus status;

}
