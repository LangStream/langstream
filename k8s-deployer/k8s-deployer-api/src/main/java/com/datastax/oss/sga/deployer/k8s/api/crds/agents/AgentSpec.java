package com.datastax.oss.sga.deployer.k8s.api.crds.agents;

import com.datastax.oss.sga.deployer.k8s.api.crds.NamespacedSpec;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class AgentSpec extends NamespacedSpec {

    private String image;
    private String imagePullPolicy;
    private String configuration;

    @Builder
    public AgentSpec(String tenant, String image, String imagePullPolicy, String configuration) {
        super(tenant);
        this.configuration = configuration;
        this.image = image;
        this.imagePullPolicy = imagePullPolicy;
    }


}
