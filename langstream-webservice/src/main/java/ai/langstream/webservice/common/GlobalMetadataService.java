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
package ai.langstream.webservice.common;

import ai.langstream.api.storage.ApplicationStore;
import ai.langstream.api.storage.GlobalMetadataStore;
import ai.langstream.api.storage.GlobalMetadataStoreRegistry;
import ai.langstream.api.webservice.tenant.CreateTenantRequest;
import ai.langstream.api.webservice.tenant.TenantConfiguration;
import ai.langstream.api.webservice.tenant.UpdateTenantRequest;
import ai.langstream.impl.storage.GlobalMetadataStoreManager;
import ai.langstream.impl.storage.tenants.TenantException;
import ai.langstream.webservice.config.StorageProperties;
import ai.langstream.webservice.config.TenantProperties;
import java.util.Map;
import lombok.SneakyThrows;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
public class GlobalMetadataService {

    private final GlobalMetadataStoreManager store;
    private final TenantProperties tenantProperties;

    public GlobalMetadataService(
            StorageProperties storageProperties,
            ApplicationStore applicationStore,
            TenantProperties tenantProperties) {
        final GlobalMetadataStore globalMetadataStore =
                GlobalMetadataStoreRegistry.loadStore(
                        storageProperties.getGlobal().getType(),
                        storageProperties.getGlobal().getConfiguration());
        store = new GlobalMetadataStoreManager(globalMetadataStore, applicationStore);
        this.tenantProperties = tenantProperties;
    }

    public void createTenant(String tenant, CreateTenantRequest request) throws TenantException {
        checkTenant(tenant);
        validateCreateTenantRequest(request);
        store.createTenant(tenant, request);
    }

    private void validateCreateTenantRequest(CreateTenantRequest request) {
        validateMaxTotalResourceUnits(request.getMaxTotalResourceUnits());
    }

    private void validateUpdateTenantRequest(UpdateTenantRequest request) {
        validateMaxTotalResourceUnits(request.getMaxTotalResourceUnits());
    }

    private void validateMaxTotalResourceUnits(Integer maxTotalResourceUnits) {
        if (tenantProperties.getMaxTotalResourceUnitsLimit() > 0
                && maxTotalResourceUnits != null
                && maxTotalResourceUnits > 0
                && maxTotalResourceUnits > tenantProperties.getMaxTotalResourceUnitsLimit()) {
            throw new IllegalArgumentException(
                    "Max total resource units limit is "
                            + tenantProperties.getMaxTotalResourceUnitsLimit());
        }
    }

    public void updateTenant(String tenant, UpdateTenantRequest request) throws TenantException {
        checkTenant(tenant);
        validateUpdateTenantRequest(request);
        store.updateTenant(tenant, request);
    }

    public void validateTenant(String tenant, boolean failIfNotExists)
            throws GlobalMetadataStoreManager.TenantNotFoundException {
        store.validateTenant(tenant, failIfNotExists);
    }

    @SneakyThrows
    public void putTenant(String tenant, TenantConfiguration tenantConfiguration) {
        checkTenant(tenant);
        store.putTenant(tenant, tenantConfiguration);
    }

    private void checkTenant(String tenant) {
        try {
            validateTenant(tenant, false);
        } catch (GlobalMetadataStoreManager.TenantNotFoundException e) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, e.getMessage());
        }
    }

    public TenantConfiguration getTenant(String tenant) {
        checkTenant(tenant);
        return store.getTenant(tenant);
    }

    @SneakyThrows
    public void deleteTenant(String tenant) {
        checkTenant(tenant);
        store.deleteTenant(tenant);
    }

    @SneakyThrows
    public Map<String, TenantConfiguration> listTenants() {
        return store.listTenants();
    }

    public void syncTenantsConfiguration() {
        store.syncTenantsConfiguration();
    }
}
