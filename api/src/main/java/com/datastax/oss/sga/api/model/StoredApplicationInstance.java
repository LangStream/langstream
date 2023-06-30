package com.datastax.oss.sga.api.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class StoredApplicationInstance {

    private String name;
    private ApplicationInstance instance;
    private ApplicationInstanceLifecycleStatus status;

}
