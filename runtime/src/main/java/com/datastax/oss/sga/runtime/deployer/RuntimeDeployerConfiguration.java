package com.datastax.oss.sga.runtime.deployer;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RuntimeDeployerConfiguration {
    private String name;
    private String application;
}
