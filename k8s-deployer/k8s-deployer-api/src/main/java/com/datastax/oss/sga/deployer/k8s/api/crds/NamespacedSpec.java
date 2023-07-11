package com.datastax.oss.sga.deployer.k8s.api.crds;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public abstract class NamespacedSpec {
    private String tenant;
}
