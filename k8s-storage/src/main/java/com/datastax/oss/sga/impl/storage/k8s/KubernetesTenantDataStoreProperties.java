package com.datastax.oss.sga.impl.storage.k8s;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class KubernetesTenantDataStoreProperties {
    private String context;
    @JsonProperty("namespaceprefix")
    private String namespacePrefix;
}
