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
package ai.langstream.impl.storage;

import ai.langstream.api.storage.ApplicationStore;
import ai.langstream.api.storage.GlobalMetadataStore;
import ai.langstream.api.webservice.tenant.CreateTenantRequest;
import ai.langstream.api.webservice.tenant.TenantConfiguration;
import ai.langstream.api.webservice.tenant.UpdateTenantRequest;
import ai.langstream.impl.storage.tenants.TenantException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Slf4j
public class GlobalMetadataStoreManager {

    private static final ObjectMapper mapper =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public static class TenantNotFoundException extends Exception {

        public TenantNotFoundException(String message) {
            super(message);
        }
    }

    protected static final String TENANT_KEY_PREFIX = "t-";
    private final GlobalMetadataStore globalMetadataStore;
    private final ApplicationStore applicationStore;

    public void syncTenantsConfiguration() {
        listTenants().keySet().forEach(this::syncTenantConfiguration);
    }

    public void syncTenantConfiguration(String tenant) {
        applicationStore.onTenantCreated(tenant);
    }

    public void validateTenant(String tenant, boolean failIfNotExists)
            throws TenantNotFoundException {
        final TenantConfiguration config = getTenant(tenant);
        if (config == null && failIfNotExists) {
            throw new TenantNotFoundException("Tenant " + tenant + " not found");
        }
        applicationStore.validateTenant(tenant, failIfNotExists);
    }

    @SneakyThrows
    public void createTenant(String tenant, CreateTenantRequest request) throws TenantException {
        final String key = keyedTenantName(tenant);
        if (globalMetadataStore.get(key) != null) {
            throw new TenantException(
                    "Tenant " + tenant + " already exists", TenantException.Type.AlreadyExists);
        }
        final TenantConfiguration configuration = createConfiguration(tenant, request);
        log.info("Creating tenant {} with configuration {}", tenant, configuration);

        globalMetadataStore.put(key, mapper.writeValueAsString(configuration));
        applicationStore.onTenantCreated(tenant);
    }

    private TenantConfiguration createConfiguration(String tenant, CreateTenantRequest request) {
        final TenantConfiguration.TenantConfigurationBuilder builder =
                TenantConfiguration.builder().name(tenant);
        final Integer maxTotalResourceUnits = request.getMaxTotalResourceUnits();
        if (maxTotalResourceUnits != null && maxTotalResourceUnits < 0) {
            throw new IllegalArgumentException("maxTotalResourceUnits must be positive");
        }
        builder.maxTotalResourceUnits(
                maxTotalResourceUnits == null ? 0 : maxTotalResourceUnits.intValue());

        final TenantConfiguration configuration = builder.build();
        return configuration;
    }

    @SneakyThrows
    public void updateTenant(String tenant, UpdateTenantRequest request) throws TenantException {
        final String key = keyedTenantName(tenant);
        final String before = globalMetadataStore.get(key);
        if (before == null) {
            throw new TenantException(
                    "Tenant " + tenant + " not found", TenantException.Type.NotFound);
        }
        final TenantConfiguration mergeConfig = mapper.readValue(before, TenantConfiguration.class);
        mergeConfiguration(request, mergeConfig);

        log.info("Updating tenant {} with configuration {}", tenant, mergeConfig);

        globalMetadataStore.put(key, mapper.writeValueAsString(mergeConfig));
        applicationStore.onTenantUpdated(tenant);
    }

    private void mergeConfiguration(UpdateTenantRequest request, TenantConfiguration mergeConfig) {
        final Integer maxTotalResourceUnits = request.getMaxTotalResourceUnits();
        if (maxTotalResourceUnits != null && maxTotalResourceUnits < 0) {
            throw new IllegalArgumentException("maxTotalResourceUnits must be positive");
        }
        mergeConfig.setMaxTotalResourceUnits(
                maxTotalResourceUnits != null ? maxTotalResourceUnits : 0);
    }

    @SneakyThrows
    public void putTenant(String tenant, TenantConfiguration tenantConfiguration) {
        final String key = keyedTenantName(tenant);
        final String before = globalMetadataStore.get(key);
        globalMetadataStore.put(key, mapper.writeValueAsString(tenantConfiguration));
        if (before != null) {
            applicationStore.onTenantUpdated(tenant);
        } else {
            applicationStore.onTenantCreated(tenant);
        }
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
        return globalMetadataStore.list().entrySet().stream()
                .filter(e -> e.getKey().startsWith(GlobalMetadataStore.TENANT_KEY_PREFIX))
                .collect(
                        Collectors.toMap(
                                e ->
                                        e.getKey()
                                                .substring(
                                                        GlobalMetadataStore.TENANT_KEY_PREFIX
                                                                .length()),
                                e -> parseTenantConfiguration(e.getValue())));
    }

    public static String keyedTenantName(String tenant) {
        return GlobalMetadataStore.TENANT_KEY_PREFIX + tenant;
    }
}
