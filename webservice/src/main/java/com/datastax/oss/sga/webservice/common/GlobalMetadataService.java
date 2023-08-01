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
package com.datastax.oss.sga.webservice.common;

import com.datastax.oss.sga.api.model.TenantConfiguration;
import com.datastax.oss.sga.api.storage.ApplicationStore;
import com.datastax.oss.sga.api.storage.GlobalMetadataStore;
import com.datastax.oss.sga.api.storage.GlobalMetadataStoreRegistry;
import com.datastax.oss.sga.impl.storage.GlobalMetadataStoreManager;
import com.datastax.oss.sga.webservice.config.StorageProperties;
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
