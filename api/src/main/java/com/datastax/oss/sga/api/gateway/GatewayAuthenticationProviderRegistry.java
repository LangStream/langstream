/**
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
