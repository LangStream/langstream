package com.datastax.oss.sga.impl.k8s;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KubernetesClientFactory {

    private static final Map<String, KubernetesClient> clients = new ConcurrentHashMap<>();

    public static KubernetesClient get(String context) {
        System.out.println("getting client for " + context + " -> " + clients);
        context = contextKey(context);
        return clients.computeIfAbsent(context, c -> new KubernetesClientBuilder()
                .withConfig(Config.autoConfigure(c.equals("__null__") ? null : c))
                .build());
    }

    private static String contextKey(String context) {
        if (context == null) {
            context = "__null__";
        }
        return context;
    }

    /**
     * Visible for testing
     * @param context
     * @return
     */
    public static void set(String context, KubernetesClient client) {
        context = contextKey(context);
        final KubernetesClient old = clients.put(context, client);
        if (old != null) {
            old.close();
        }
    }

    /**
     * Visible for testing
     * @return
     */
    public static void clear() {
        clients.values().forEach(KubernetesClient::close);
        clients.clear();
    }

    private KubernetesClientFactory() {
    }
}
