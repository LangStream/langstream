package com.datastax.oss.sga.impl.storage.k8s.apps;

import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.model.StoredApplicationInstance;
import com.datastax.oss.sga.api.storage.ApplicationStore;
import com.datastax.oss.sga.deployer.k8s.client.DeployerClient;
import com.datastax.oss.sga.impl.storage.k8s.KubernetesClientFactory;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class KubernetesApplicationStoreProperties {
    private String namespaceprefix;
}
