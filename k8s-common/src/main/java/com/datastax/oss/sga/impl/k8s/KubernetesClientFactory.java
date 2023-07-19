package com.datastax.oss.sga.impl.k8s;

import io.fabric8.kubernetes.api.model.NamedContext;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KubernetesClientFactory {

    private static final Map<String, KubernetesClient> clients = new ConcurrentHashMap<>();

    public static KubernetesClient get(String context) {
        context = contextKey(context);
        return clients.computeIfAbsent(context, c -> {
            final Config config = Config.autoConfigure(c.equals("__null__") ? null : c);
            try {
                log.info("Creating kubernetes client for server {}", config.getMasterUrl());
                final KubernetesClient newClient = new KubernetesClientBuilder()
                        .withConfig(config)
                        .build();
                // https://kubernetes.io/docs/reference/using-api/health-checks/
                final String livez = newClient.raw("/livez");
                if (livez == null) {
                    // 404 means the health check is not implemented
                    log.warn(
                            "/livez endpoint not available on server {}, cannot determine if the server is up and running",
                            config.getMasterUrl());
                }
                log.info("Created kubernetes client for server {}", config.getMasterUrl());
                return newClient;
            } catch (KubernetesClientException ex) {
                log.error("Cannot create kubernetes client for server {}, is kubernetes up and running?",
                        config.getMasterUrl(), ex);
                throw new RuntimeException("Cannot create kubernetes client for server "
                        + config.getMasterUrl() + ", is kubernetes up and running?");
            } catch (Throwable ex) {
                log.error("Cannot create kubernetes client for server {}, is kubernetes up and running?",
                        config.getMasterUrl(), ex);
                throw new RuntimeException(ex);
            }

        });
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
