package com.datastax.oss.sga.api.gateway;

import com.datastax.oss.sga.api.storage.GlobalMetadataStore;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;

public class GatewayAuthenticationProviderRegistry {


    public static GatewayAuthenticationProvider loadProvider(String type, Map<String, Object> configuration) {
        Objects.requireNonNull(type, "type cannot be null");
        Objects.requireNonNull(configuration, "configuration cannot be null");
        ServiceLoader<GatewayAuthenticationProvider> loader = ServiceLoader.load(GatewayAuthenticationProvider.class);
        final GatewayAuthenticationProvider store = loader
                .stream()
                .filter(p -> {
                    return type.equals(p.get().type());
                })
                .findFirst()
                .orElseThrow(
                        () -> new RuntimeException("No GatewayAuthenticationProvider found for type " + type)
                )
                .get();
        store.initialize(configuration);
        return store;
    }
}
