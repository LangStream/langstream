package com.datastax.oss.sga.deployer.k8s.api.crds.agents;

import com.datastax.oss.sga.deployer.k8s.api.crds.NamespacedSpec;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class AgentSpec extends NamespacedSpec {

    public record Resources(int parallelism, int size) {}

    private String agentId;
    private String applicationId;
    private String image;
    private String imagePullPolicy;
    private String agentConfigSecretRef;
    private String agentConfigSecretRefChecksum;
    private Resources resources;
    private String codeArchiveId;


    @Builder
    public AgentSpec(String tenant, String agentId,
                     String applicationId,
                     String image,
                     String imagePullPolicy,
                     String agentConfigSecretRef,
                     String agentConfigSecretRefChecksum,
                     Resources resources,
                     String codeArchiveId) {
        super(tenant);
        this.agentId = agentId;
        this.applicationId = applicationId;
        this.image = image;
        this.imagePullPolicy = imagePullPolicy;
        this.agentConfigSecretRef = agentConfigSecretRef;
        this.agentConfigSecretRefChecksum = agentConfigSecretRefChecksum;
        this.resources = resources;
        this.codeArchiveId = codeArchiveId;
    }


}
