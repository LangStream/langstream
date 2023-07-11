package com.datastax.oss.sga.runtime.impl.k8s;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class KubernetesClusterRuntimeConfiguration {

    private Map<String, Object> admin;
    private String defaultTenant = "public";
    private String defaultNamespace = "default";

}
