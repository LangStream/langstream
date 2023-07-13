package com.datastax.oss.sga.runtime.impl.k8s;

import com.fasterxml.jackson.annotation.JsonAlias;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class KubernetesClusterRuntimeConfiguration {

    @JsonAlias({"namespace-prefix", "namespaceprefix"})
    private String namespacePrefix;
    private String image;
    @JsonAlias({"image-pull-policy", "imagepullpolicy"})
    private String imagePullPolicy;

}
