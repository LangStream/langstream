package com.datastax.oss.sga.impl.storage.k8s;

import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

public class SecretsKubernetesConfigStore extends AbstractKubernetesConfigStore<Secret> {

    @Override
    public boolean acceptScope(Scope scope) {
        return scope == Scope.SECRETS;
    }

    @Override
    protected Secret createResource(String key, String value) {
        return new SecretBuilder()
                .withData(Map.of("value", encode(value)))
                .build();
    }

    private static String encode(String value) {
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    protected MixedOperation<Secret, ? extends KubernetesResourceList<Secret>, Resource<Secret>> operation() {
        return client.secrets();
    }

    @Override
    protected String get(String key, Secret resource) {
        final String rawValue = resource.getData().get("value");
        return decode(rawValue);
    }

    private static String decode(String rawValue) {
        return new String(Base64.getDecoder().decode(rawValue), StandardCharsets.UTF_8);
    }
}
