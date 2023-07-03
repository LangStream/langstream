package com.datastax.oss.sga.pulsar;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PulsarClusterRuntimeConfiguration {

    private Map<String, Object> admin;
    private String defaultTenant = "public";
    private String defaultNamespace = "default";

}
