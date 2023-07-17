package com.datastax.oss.sga.pulsar;

import com.datastax.oss.sga.api.model.StreamingCluster;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;

import java.util.HashMap;
import java.util.Map;

public class PulsarClientUtils {
    public static PulsarAdmin buildPulsarAdmin(StreamingCluster streamingCluster) throws Exception {
        final PulsarClusterRuntimeConfiguration pulsarClusterRuntimeConfiguration =
                getPulsarClusterRuntimeConfiguration(streamingCluster);
        Map<String, Object> adminConfig = pulsarClusterRuntimeConfiguration.getAdmin();
        if (adminConfig == null) {
            adminConfig = new HashMap<>();
        } else {
            adminConfig = new HashMap<>(adminConfig);
        }
        if (pulsarClusterRuntimeConfiguration.getAuthentication() != null) {
            adminConfig.putAll(pulsarClusterRuntimeConfiguration.getAuthentication());
        }
        if (adminConfig.get("serviceUrl") == null) {
            adminConfig.put("serviceUrl", "http://localhost:8080");
        }
        return PulsarAdmin
                .builder()
                .loadConf(adminConfig)
                .build();
    }

    public static PulsarClient buildPulsarClient(StreamingCluster streamingCluster) throws Exception {
        final PulsarClusterRuntimeConfiguration pulsarClusterRuntimeConfiguration =
                getPulsarClusterRuntimeConfiguration(streamingCluster);
        Map<String, Object> clientConfig = pulsarClusterRuntimeConfiguration.getService();
        if (clientConfig == null) {
            clientConfig = new HashMap<>();
        } else {
            clientConfig = new HashMap<>(clientConfig);
        }
        if (pulsarClusterRuntimeConfiguration.getAuthentication() != null) {
            clientConfig.putAll(pulsarClusterRuntimeConfiguration.getAuthentication());
        }
        if (clientConfig.get("serviceUrl") == null) {
            clientConfig.put("serviceUrl", "pulsar://localhost:6650");
        }
        return PulsarClient
                .builder()
                .loadConf(clientConfig)
                .build();
    }

    public static PulsarClusterRuntimeConfiguration getPulsarClusterRuntimeConfiguration(StreamingCluster streamingCluster) {
        final Map<String, Object> configuration = streamingCluster.configuration();
        return PulsarClusterRuntime.mapper.convertValue(configuration, PulsarClusterRuntimeConfiguration.class);
    }
}
