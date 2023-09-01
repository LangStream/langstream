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
package ai.langstream.deployer.k8s;

import ai.langstream.api.storage.GlobalMetadataStore;
import ai.langstream.api.storage.GlobalMetadataStoreRegistry;
import ai.langstream.api.webservice.tenant.TenantConfiguration;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.SneakyThrows;
import lombok.extern.jbosslog.JBossLog;

@ApplicationScoped
@JBossLog
public class TenantsConfigurationReader {
    private static final ObjectMapper mapper =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final GlobalMetadataStore globalMetadataStore;

    public TenantsConfigurationReader(ResolvedDeployerConfiguration resolvedDeployerConfiguration) {
        final GlobalStorageConfiguration configuration =
                resolvedDeployerConfiguration.getGlobalStorageConfiguration();
        if (configuration == null || configuration.getType() == null) {
            log.warnf("No global storage configuration found. Tenants metadata won't be used.");
            this.globalMetadataStore = null;
        } else {
            this.globalMetadataStore =
                    GlobalMetadataStoreRegistry.loadStore(
                            configuration.getType(), configuration.getConfiguration());
        }
    }

    public TenantConfiguration getTenantConfiguration(String tenant) {
        if (globalMetadataStore == null) {
            return null;
        }
        final String config =
                globalMetadataStore.get(GlobalMetadataStore.TENANT_KEY_PREFIX + tenant);
        if (config == null) {
            return null;
        }
        return parseTenantConfiguration(config);
    }

    @SneakyThrows
    private TenantConfiguration parseTenantConfiguration(String res) {
        return mapper.readValue(res, TenantConfiguration.class);
    }
}
