package com.datastax.oss.sga.impl.storage.k8s;

import com.datastax.oss.sga.api.storage.TenantDataStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractKubernetesTenantDataStore<T extends HasMetadata> extends AbstractKubernetesGenericStore<T> implements TenantDataStore {

    static final ObjectMapper mapper = new ObjectMapper();

    protected KubernetesClient client;
    private String namespacePrefix;

    @Override
    public String storeType() {
        return "kubernetes";
    }


    @Override
    public void initialize(Map<String, String> configuration) {
        final KubernetesTenantDataStoreProperties properties =
                mapper.convertValue(configuration, KubernetesTenantDataStoreProperties.class);
        namespacePrefix = properties.getNamespacePrefix();
        Objects.requireNonNull("namespace prefix is required");
        client = KubernetesClientFactory.get(properties.getContext());
        log.info("Kubernetes client configured");
    }

    @Override
    @SneakyThrows
    public void initializeTenant(String tenant) {
        final String ns = getTenantNamespace(tenant);
        if (client.namespaces().withName(ns).get() == null) {
            final Namespace namespace = new NamespaceBuilder()
                    .withNewMetadata()
                    .withName(ns)
                    .endMetadata()
                    .build();
            client.resource(namespace).serverSideApply();
        }
    }

    private String getTenantNamespace(String tenant) {
        return "%s%s".formatted(namespacePrefix, tenant);
    }

    @Override
    protected KubernetesClient getClient() {
        return client;
    }

    @Override
    @SneakyThrows
    public void put(String tenant, String key, String value) {
        final String namespace = getTenantNamespace(tenant);
        putInNamespace(namespace, key, value);
    }

    @Override
    @SneakyThrows
    public void delete(String tenant, String key) {
        deleteInNamespace(getTenantNamespace(tenant), key);
    }

    @Override
    @SneakyThrows
    public String get(String tenant, String key) {
        return getInNamespace(getTenantNamespace(tenant), key);
    }

    @Override
    @SneakyThrows
    public LinkedHashMap<String, String> list(String tenant) {
        return listInNamespace(getTenantNamespace(tenant));
    }

}
