package com.datastax.oss.sga.impl.storage.k8s;

import com.datastax.oss.sga.api.storage.ConfigStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractKubernetesConfigStore<T extends HasMetadata> implements ConfigStore {

    static final ObjectMapper mapper = new ObjectMapper();
    protected static final String PREFIX = "sga-";

    protected KubernetesClient client;
    private String namespace;

    @Override
    public String storeType() {
        return "kubernetes";
    }


    @Override
    public void initialize(Map<String, String> configuration) {
        final KubernetesConfigStoreProperties properties =
                mapper.convertValue(configuration, KubernetesConfigStoreProperties.class);
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
        log.info("Kubernetes client configured");
    }

    private static String resourceName(String key) {
        return PREFIX + key;
    }

    @Override
    @SneakyThrows
    public void put(String key, String value) {
        final T resource = createResource(key, value);
        final ObjectMeta metadata = new ObjectMetaBuilder()
                .withName(resourceName(key))
                .withLabels(Map.of("app", "sga", "sga-key", key))
                .build();
        resource.setMetadata(metadata);
        client.resource(resource).inNamespace(namespace).serverSideApply();
    }

    protected abstract T createResource(String key, String value);

    protected abstract MixedOperation<T, ? extends KubernetesResourceList<T>, Resource<T>> operation();

    protected abstract String get(String key, T resource);


    @Override
    @SneakyThrows
    public void delete(String key) {
        operation().inNamespace(namespace).withName(resourceName(key)).delete();
    }

    @Override
    @SneakyThrows
    public String get(String key) {
        final T resource = operation().inNamespace(namespace).withName(resourceName(key)).get();
        if (resource == null) {
            return null;
        }
        return get(key, resource);
    }

    @Override
    @SneakyThrows
    public LinkedHashMap<String, String> list() {
        return operation().inNamespace(namespace).withLabel("app", "sga").list().getItems().stream()
                .collect(Collectors.toMap(cf -> cf.getMetadata().getLabels().get("sga-key"),
                        cf -> get(cf.getMetadata().getLabels().get("sga-key"), cf), (a, b) -> b, LinkedHashMap::new));
    }
}
