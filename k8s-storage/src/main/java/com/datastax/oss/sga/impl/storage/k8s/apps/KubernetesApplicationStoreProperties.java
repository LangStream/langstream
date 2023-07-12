package com.datastax.oss.sga.impl.storage.k8s.apps;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class KubernetesApplicationStoreProperties {
    private String namespaceprefix;

    @Data
    @NoArgsConstructor
    public static class DeployerRuntimeConfig {
        private String image;
        @JsonProperty("image-pull-policy")
        private String imagePullPolicy;
    }


    @JsonProperty("deployer-runtime")
    private DeployerRuntimeConfig deployerRuntime;

}
