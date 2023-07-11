package com.datastax.oss.sga.deployer.k8s.api.crds;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@NoArgsConstructor
@SuperBuilder
@AllArgsConstructor
public abstract class NamespacedSpec {
    private String tenant;
}
