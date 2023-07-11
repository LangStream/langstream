package com.datastax.oss.sga.deployer.k8s.api.crds.apps;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import lombok.Data;

@Data
public class ApplicationStatus {
    @JsonPropertyDescription("Last spec applied.")
    String lastApplied;

}
