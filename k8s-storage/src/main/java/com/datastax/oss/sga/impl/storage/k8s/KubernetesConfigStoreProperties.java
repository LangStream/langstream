package com.datastax.oss.sga.impl.storage.k8s;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class KubernetesConfigStoreProperties {
    private String context;
    private String namespace;
}
