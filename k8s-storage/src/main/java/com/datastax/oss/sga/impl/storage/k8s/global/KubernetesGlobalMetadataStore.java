package com.datastax.oss.sga.impl.storage.k8s.global;

import com.datastax.oss.sga.api.storage.GlobalMetadataStore;
import com.datastax.oss.sga.impl.storage.k8s.AbstractKubernetesGenericStore;
import com.datastax.oss.sga.impl.storage.k8s.KubernetesClientFactory;
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
                throw new IllegalArgumentException("Kubernetes namespace is required. Either set " +
                        "KUBERNETES_NAMESPACE environment variable or set namespace in configuration.");
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
        return new ConfigMapBuilder()
                .withData(Map.of("value", value))
                .build();
    }

    @Override
    protected MixedOperation<ConfigMap, ? extends KubernetesResourceList<ConfigMap>, Resource<ConfigMap>> operation() {
        return client.configMaps();
    }

    @Override
    protected String get(String key, ConfigMap resource) {
        return resource.getData().get("value");
    }


}
