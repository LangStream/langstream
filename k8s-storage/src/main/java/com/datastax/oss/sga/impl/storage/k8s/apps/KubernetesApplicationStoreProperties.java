package com.datastax.oss.sga.impl.storage.k8s.apps;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class KubernetesApplicationStoreProperties {
    private String namespaceprefix;
}
