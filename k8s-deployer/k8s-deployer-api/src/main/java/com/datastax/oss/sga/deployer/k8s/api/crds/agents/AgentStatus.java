package com.datastax.oss.sga.deployer.k8s.api.crds.agents;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import lombok.Data;

@Data
public class AgentStatus {
    @JsonPropertyDescription("Last spec applied.")
    String lastApplied;

}
