package com.datastax.oss.sga.webservice.application;

import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.model.StoredApplicationInstance;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeRegistry;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.api.storage.ConfigStore;
import com.datastax.oss.sga.api.storage.ConfigStoreRegistry;
import com.datastax.oss.sga.impl.deploy.ApplicationDeployer;
import com.datastax.oss.sga.impl.deploy.ApplicationManager;
import com.datastax.oss.sga.impl.storage.ApplicationStore;
import com.datastax.oss.sga.webservice.config.SchedulingProperties;
import com.datastax.oss.sga.webservice.config.StorageProperties;
import java.util.Map;
import lombok.SneakyThrows;
import org.springframework.stereotype.Service;

@Service
public class ApplicationService {

    private final ApplicationManager applicationManager;

    public ApplicationService(StorageProperties storageProperties, SchedulingProperties schedulingProperties) {
        final ConfigStore configStore =
                ConfigStoreRegistry.loadConfigsStore(storageProperties.getConfigs().getType(),
                        storageProperties.getConfigs().getConfiguration());
        final ConfigStore secretsStore =
                ConfigStoreRegistry.loadSecretsStore(storageProperties.getSecrets().getType(),
                        storageProperties.getSecrets().getConfiguration());

        final ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .pluginsRegistry(new PluginsRegistry())
                .registry(new ClusterRuntimeRegistry())
                .build();
        final ApplicationStore store = new ApplicationStore(configStore, secretsStore);
        applicationManager = new ApplicationManager(deployer, store, schedulingProperties.getExecutors());
    }

    @SneakyThrows
    public Map<String, StoredApplicationInstance> getAllApplications() {
        return applicationManager.getAllApplications();
    }

    @SneakyThrows
    public void deployApplication(String applicationName, ApplicationInstance applicationInstance) {
        applicationManager.deployApplication(applicationName, applicationInstance);
    }

    @SneakyThrows
    public StoredApplicationInstance getApplication(String applicationName) {
        return applicationManager.getApplication(applicationName);
    }

    @SneakyThrows
    public void deleteApplication(String applicationName) {
        applicationManager.deleteApplication(applicationName);
    }

}
