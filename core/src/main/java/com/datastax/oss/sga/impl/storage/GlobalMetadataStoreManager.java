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
package com.datastax.oss.sga.impl.storage;

import com.datastax.oss.sga.api.model.TenantConfiguration;
import com.datastax.oss.sga.api.storage.ApplicationStore;
import com.datastax.oss.sga.api.storage.GlobalMetadataStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

@AllArgsConstructor
public class GlobalMetadataStoreManager {

    private static final ObjectMapper mapper = new ObjectMapper();
    protected static final String TENANT_KEY_PREFIX = "t-";
    private final GlobalMetadataStore globalMetadataStore;
    private final ApplicationStore applicationStore;

    @SneakyThrows
    public void putTenant(String tenant, TenantConfiguration tenantConfiguration) {
        final String key = keyedTenantName(tenant);
        globalMetadataStore.put(key, mapper.writeValueAsString(tenantConfiguration));
        applicationStore.onTenantCreated(tenant);
    }

    public TenantConfiguration getTenant(String tenant) {
        final String key = keyedTenantName(tenant);
        final String res = globalMetadataStore.get(key);
        if (res != null) {
            return parseTenantConfiguration(res);
        }
        return null;
    }

    @SneakyThrows
    private TenantConfiguration parseTenantConfiguration(String res) {
        return mapper.readValue(res, TenantConfiguration.class);
    }

    @SneakyThrows
    public void deleteTenant(String tenant) {
        final String key = keyedTenantName(tenant);
        globalMetadataStore.delete(key);
        applicationStore.onTenantDeleted(tenant);
    }

    @SneakyThrows
    public Map<String, TenantConfiguration> listTenants() {
        return globalMetadataStore.list()
                .entrySet()
                .stream()
                .filter(e -> e.getKey().startsWith(TENANT_KEY_PREFIX))
                .collect(Collectors.toMap(
                        e -> e.getKey().substring(TENANT_KEY_PREFIX.length()),
                        e -> parseTenantConfiguration(e.getValue()))
                );
    }

    private static String keyedTenantName(String tenant) {
        return TENANT_KEY_PREFIX + tenant;
    }

}
