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
package ai.langstream.webservice.common;

import ai.langstream.webservice.config.StorageProperties;
import ai.langstream.api.model.TenantConfiguration;
import ai.langstream.api.storage.ApplicationStore;
import ai.langstream.api.storage.GlobalMetadataStore;
import ai.langstream.api.storage.GlobalMetadataStoreRegistry;
import ai.langstream.impl.storage.GlobalMetadataStoreManager;

import java.util.Map;
import lombok.SneakyThrows;
import org.springframework.stereotype.Service;

@Service
public class GlobalMetadataService {

    private final GlobalMetadataStoreManager store;

    public GlobalMetadataService(StorageProperties storageProperties, ApplicationStore applicationStore) {
        final GlobalMetadataStore globalMetadataStore =
                GlobalMetadataStoreRegistry.loadStore(storageProperties.getGlobal().getType(),
                        storageProperties.getGlobal().getConfiguration());
        store = new GlobalMetadataStoreManager(globalMetadataStore, applicationStore);
    }

    @SneakyThrows
    public void putTenant(String tenant, TenantConfiguration tenantConfiguration) {
        store.putTenant(tenant, tenantConfiguration);
    }

    public TenantConfiguration getTenant(String tenant) {
        return store.getTenant(tenant);
    }

    @SneakyThrows
    public void deleteTenant(String tenant) {
        store.deleteTenant(tenant);
    }

    @SneakyThrows
    public Map<String, TenantConfiguration> listTenants() {
        return store.listTenants();
    }

}
