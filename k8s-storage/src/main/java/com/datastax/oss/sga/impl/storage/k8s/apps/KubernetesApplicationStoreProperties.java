package com.datastax.oss.sga.impl.storage.k8s.apps;

import com.fasterxml.jackson.annotation.JsonAlias;
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
        @JsonAlias({"image-pull-policy", "imagepullpolicy"})
        private String imagePullPolicy;
    }


    @JsonAlias({"deployer-runtime", "deployerruntime"})
    private DeployerRuntimeConfig deployerRuntime;

}
