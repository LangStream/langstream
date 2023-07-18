package com.datastax.oss.sga.deployer.k8s.api.crds.agents;

import com.datastax.oss.sga.api.model.AgentLifecycleStatus;
import com.datastax.oss.sga.deployer.k8s.api.crds.BaseStatus;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import lombok.Data;

@Data
public class AgentStatus extends BaseStatus {
    private AgentLifecycleStatus status;
}
