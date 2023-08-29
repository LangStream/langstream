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
package ai.langstream.impl.storage.k8s.global;

import ai.langstream.api.storage.GlobalMetadataStore;
import ai.langstream.impl.k8s.KubernetesClientFactory;
import ai.langstream.impl.storage.k8s.AbstractKubernetesGenericStore;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KubernetesGlobalMetadataStore extends AbstractKubernetesGenericStore<ConfigMap>
        implements GlobalMetadataStore {

    private KubernetesClient client;
    private String namespace;

    @Override
    public void initialize(Map<String, Object> configuration) {
        final KubernetesGlobalMetadataStoreProperties properties =
                mapper.convertValue(configuration, KubernetesGlobalMetadataStoreProperties.class);

        if (properties.getNamespace() == null) {
            final String fromEnv = System.getenv("KUBERNETES_NAMESPACE");
            if (fromEnv == null) {
                throw new IllegalArgumentException(
                        "Kubernetes namespace is required. Either set "
                                + "KUBERNETES_NAMESPACE environment variable or set namespace in configuration.");
            } else {
                namespace = fromEnv;
                log.info("Using namespace from env {}", namespace);
            }
        } else {
            namespace = properties.getNamespace();
            log.info("Using namespace from properties {}", namespace);
        }
        client = KubernetesClientFactory.get(properties.getContext());
    }

    @Override
    protected KubernetesClient getClient() {
        return client;
    }

    @Override
    public void put(String key, String value) {
        putInNamespace(namespace, key, value);
    }

    @Override
    public void delete(String key) {
        deleteInNamespace(namespace, key);
    }

    @Override
    public String get(String key) {
        return getInNamespace(namespace, key);
    }

    @Override
    public LinkedHashMap<String, String> list() {
        return listInNamespace(namespace);
    }

    @Override
    protected ConfigMap createResource(String key, String value) {
        return new ConfigMapBuilder().withData(Map.of("value", value)).build();
    }

    @Override
    protected MixedOperation<
                    ConfigMap, ? extends KubernetesResourceList<ConfigMap>, Resource<ConfigMap>>
            operation() {
        return client.configMaps();
    }

    @Override
    protected String get(String key, ConfigMap resource) {
        return resource.getData().get("value");
    }
}
