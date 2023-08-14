package com.datastax.oss.sga.deployer.k8s;

import io.fabric8.kubernetes.api.model.Toleration;
import java.util.List;
import java.util.Map;

public record PodTemplate(List<Toleration> tolerations, Map<String, String> nodeSelector) {
}
