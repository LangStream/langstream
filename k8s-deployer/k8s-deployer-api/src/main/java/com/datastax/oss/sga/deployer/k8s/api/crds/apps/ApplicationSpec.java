package com.datastax.oss.sga.deployer.k8s.api.crds.apps;

import com.datastax.oss.sga.deployer.k8s.api.crds.NamespacedSpec;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ApplicationSpec extends NamespacedSpec {
    private String image;
    private String imagePullPolicy;
    private String application;

    private String codeArchiveId;

    @Builder
    public ApplicationSpec(String tenant, String image, String imagePullPolicy, String application, String codeArchiveId) {
        super(tenant);
        this.image = image;
        this.imagePullPolicy = imagePullPolicy;
        this.application = application;
    }
}
