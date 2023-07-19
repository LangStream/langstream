package com.datastax.oss.sga.deployer.k8s.agents;

import lombok.Data;

@Data
public class AgentResourceUnitConfiguration {

    private float cpuPerUnit = 0.5f;
    // MB
    private long memPerUnit = 256;
    private int instancePerUnit = 1;

    private int defaultCpuMemUnits = 1;
    private int defaultInstanceUnits = 1;

    private int maxCpuMemUnits = 8;
    private int maxInstanceUnits = 8;

}
