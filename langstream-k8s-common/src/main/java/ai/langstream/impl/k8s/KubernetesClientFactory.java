/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.langstream.impl.k8s;

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

    public static KubernetesClient create(String context) {
        if ("__null__".equals(context)) {
            context = null;
        }
        final Config config = Config.autoConfigure(context);
        try {
            log.info("Creating kubernetes client for server {}", config.getMasterUrl());
            final KubernetesClient newClient =
                    new KubernetesClientBuilder().withConfig(config).build();
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
            log.error(
                    "Cannot create kubernetes client for server {}, is kubernetes up and running?",
                    config.getMasterUrl(),
                    ex);
            throw new RuntimeException(
                    "Cannot create kubernetes client for server "
                            + config.getMasterUrl()
                            + ", is kubernetes up and running?");
        } catch (Throwable ex) {
            log.error(
                    "Cannot create kubernetes client for server {}, is kubernetes up and running?",
                    config.getMasterUrl(),
                    ex);
            throw new RuntimeException(ex);
        }
    }

    public static KubernetesClient get(String context) {
        context = contextKey(context);
        return clients.computeIfAbsent(context, KubernetesClientFactory::create);
    }

    private static String contextKey(String context) {
        if (context == null) {
            context = "__null__";
        }
        return context;
    }

    private KubernetesClientFactory() {}
}
