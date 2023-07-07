package com.datastax.oss.sga.webservice.application;

import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.model.StoredApplicationInstance;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeRegistry;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.impl.deploy.ApplicationDeployer;
import com.datastax.oss.sga.impl.deploy.ApplicationManager;
import com.datastax.oss.sga.impl.storage.ApplicationStore;
import com.datastax.oss.sga.webservice.common.GlobalMetadataService;
import com.datastax.oss.sga.webservice.config.SchedulingProperties;
import java.util.Map;
import java.util.Set;
import lombok.SneakyThrows;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
public class ApplicationService {

    private final ApplicationManager applicationManager;
    private final GlobalMetadataService globalMetadataService;

    public ApplicationService(
            GlobalMetadataService globalMetadataService,
            SchedulingProperties schedulingProperties,
            ApplicationStore store) {
        this.globalMetadataService = globalMetadataService;
        final ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .pluginsRegistry(new PluginsRegistry())
                .registry(new ClusterRuntimeRegistry())
                .build();

        final Set<String> tenants = globalMetadataService.listTenants()
                .keySet();
        tenants.forEach(store::loadTenant);
        applicationManager = new ApplicationManager(deployer, store, schedulingProperties.getExecutors());
        tenants.forEach(applicationManager::recoverTenant);
    }


    @SneakyThrows
    public Map<String, StoredApplicationInstance> getAllApplications(String tenant) {
        checkTenant(tenant);
        return applicationManager.getAllApplications(tenant);
    }

    @SneakyThrows
    public void deployApplication(String tenant, String applicationName, ApplicationInstance applicationInstance) {
        checkTenant(tenant);
        applicationManager.deployApplication(tenant, applicationName, applicationInstance);
    }

    @SneakyThrows
    public StoredApplicationInstance getApplication(String tenant, String applicationName) {
        checkTenant(tenant);
        return applicationManager.getApplication(tenant, applicationName);
    }

    @SneakyThrows
    public void deleteApplication(String tenant, String applicationName) {
        checkTenant(tenant);
        applicationManager.deleteApplication(tenant, applicationName);
    }

    private void checkTenant(String tenant) {
        if (globalMetadataService.getTenant(tenant) == null) {
            throw new ResponseStatusException(
                    HttpStatus.NOT_FOUND, "tenant not found"
            );
        }
    }

}
