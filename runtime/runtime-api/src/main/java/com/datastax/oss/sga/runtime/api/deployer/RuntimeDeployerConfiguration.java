package com.datastax.oss.sga.runtime.api.deployer;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RuntimeDeployerConfiguration {
    private String applicationId;
    private String tenant;
    private String application;
}
