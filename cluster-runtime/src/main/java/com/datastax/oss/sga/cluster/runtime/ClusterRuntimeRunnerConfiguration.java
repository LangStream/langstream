package com.datastax.oss.sga.cluster.runtime;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ClusterRuntimeRunnerConfiguration {

    private String name;
    private String application;
}
