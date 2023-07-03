package com.datastax.oss.sga.webservice.application;

import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.model.ApplicationInstanceLifecycleStatus;
import com.datastax.oss.sga.api.model.StoredApplicationInstance;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeRegistry;
import com.datastax.oss.sga.api.runtime.PhysicalApplicationInstance;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.api.storage.ConfigStore;
import com.datastax.oss.sga.api.storage.ConfigStoreRegistry;
import com.datastax.oss.sga.impl.deploy.ApplicationDeployer;
import com.datastax.oss.sga.impl.storage.ApplicationStore;
import com.datastax.oss.sga.webservice.config.StorageProperties;
import java.util.Map;
import lombok.SneakyThrows;
import org.springframework.stereotype.Service;

@Service
public class ApplicationService {

    private final ApplicationDeployer<PhysicalApplicationInstance> deployer = ApplicationDeployer
            .builder()
            .pluginsRegistry(new PluginsRegistry())
            .registry(new ClusterRuntimeRegistry())
            .build();

    private final StorageProperties storageProperties;
    private final ApplicationStore applicationStore;

    public ApplicationService(StorageProperties storageProperties) {
        this.storageProperties = storageProperties;
        final ConfigStore configStore =
                ConfigStoreRegistry.loadConfigStore(storageProperties.getType(),
                        storageProperties.getConfiguration());
        this.applicationStore = new ApplicationStore(configStore, configStore);
    }

    @SneakyThrows
    public Map<String, StoredApplicationInstance> getAllApplications() {
        return applicationStore.list();
    }

    @SneakyThrows
    public void deployApplication(String applicationName, ApplicationInstance applicationInstance) {
        applicationStore.put(applicationName, applicationInstance, ApplicationInstanceLifecycleStatus.CREATED);

        try {
            final PhysicalApplicationInstance implementation = deployer.createImplementation(applicationInstance);
            deployer.deploy(applicationInstance, implementation);
            applicationStore.put(applicationName, applicationInstance, ApplicationInstanceLifecycleStatus.DEPLOYED);
        } catch (Exception e) {
            applicationStore.put(applicationName, applicationInstance, ApplicationInstanceLifecycleStatus.error(e.getMessage()));
            throw new RuntimeException(e);
        }
    }

    @SneakyThrows
    public StoredApplicationInstance getApplication(String applicationName) {
        return applicationStore.get(applicationName);
    }

    @SneakyThrows
    public void deleteApplication(String applicationName) {
        final StoredApplicationInstance current = applicationStore.get(applicationName);
        if (current == null || current.getStatus() == ApplicationInstanceLifecycleStatus.DELETING) {
            return;
        }
        applicationStore.put(applicationName, current.getInstance(), ApplicationInstanceLifecycleStatus.DELETING);
        // not supported yet
        // deployer.delete(applicationInstance, deployer.createImplementation(current.getInstance()));
        applicationStore.delete(applicationName);
    }

}
