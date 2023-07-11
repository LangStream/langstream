package com.datastax.oss.sga.impl.storage.k8s;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KubernetesClientFactory {

    private static final Map<String, KubernetesClient> clients = new ConcurrentHashMap<>();


    public static KubernetesClient get(String context) {
        if (context == null) {
            context = "__null__";
        }
        return clients.computeIfAbsent(context, c -> new KubernetesClientBuilder()
                .withConfig(Config.autoConfigure(c.equals("__null__") ? null : c))
                .build());
    }

    private KubernetesClientFactory() {
    }
}
