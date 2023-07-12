package com.datastax.oss.sga.impl.storage.k8s.global;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class KubernetesGlobalMetadataStoreProperties {
    private String context;
    private String namespace;
}
