package com.datastax.oss.sga.deployer.k8s.crds;

import lombok.Data;

@Data
public class ApplicationSpec {
    private String name;
}
