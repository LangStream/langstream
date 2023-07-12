package com.datastax.oss.sga.deployer.k8s.api.crds.apps;

import com.datastax.oss.sga.deployer.k8s.api.crds.NamespacedSpec;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ApplicationSpec extends NamespacedSpec {
    private String image;
    private String imagePullPolicy;
    private String application;

}
