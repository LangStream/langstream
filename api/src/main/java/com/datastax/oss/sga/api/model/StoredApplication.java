package com.datastax.oss.sga.api.model;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class StoredApplication {

    private String applicationId;
    private Application instance;
    private ApplicationStatus status;


}
