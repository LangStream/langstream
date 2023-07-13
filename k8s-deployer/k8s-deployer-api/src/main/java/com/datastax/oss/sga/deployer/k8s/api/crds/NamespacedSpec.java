package com.datastax.oss.sga.deployer.k8s.api.crds;

import io.fabric8.generator.annotation.Required;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@NoArgsConstructor
@AllArgsConstructor
public abstract class NamespacedSpec {
    @Required
    String tenant;
}
