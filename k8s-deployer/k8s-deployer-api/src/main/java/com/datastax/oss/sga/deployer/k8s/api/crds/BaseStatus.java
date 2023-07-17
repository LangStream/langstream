package com.datastax.oss.sga.deployer.k8s.api.crds;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class BaseStatus {

    @JsonPropertyDescription("Last spec applied.")
    String lastApplied;
}
