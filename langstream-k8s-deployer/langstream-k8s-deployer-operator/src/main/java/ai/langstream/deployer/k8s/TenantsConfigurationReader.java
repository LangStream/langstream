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
        if (configuration == null) {
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
