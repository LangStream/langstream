package com.datastax.oss.sga.webservice.application;

import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.model.StoredApplicationInstance;
import com.datastax.oss.sga.api.storage.ApplicationStore;
import com.datastax.oss.sga.webservice.common.GlobalMetadataService;
import java.util.Map;
import lombok.SneakyThrows;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
public class ApplicationService {

    private final GlobalMetadataService globalMetadataService;
    private final ApplicationStore applicationStore;

    public ApplicationService(
            GlobalMetadataService globalMetadataService,
            ApplicationStore store) {
        this.globalMetadataService = globalMetadataService;
        this.applicationStore = store;
    }


    @SneakyThrows
    public Map<String, StoredApplicationInstance> getAllApplications(String tenant) {
        checkTenant(tenant);
        return applicationStore.list(tenant);
    }

    @SneakyThrows
    public void deployApplication(String tenant, String applicationName, ApplicationInstance applicationInstance) {
        checkTenant(tenant);
        applicationStore.put(tenant, applicationName, applicationInstance);
    }

    @SneakyThrows
    public StoredApplicationInstance getApplication(String tenant, String applicationName) {
        checkTenant(tenant);
        return applicationStore.get(tenant, applicationName);
    }

    @SneakyThrows
    public void deleteApplication(String tenant, String applicationName) {
        checkTenant(tenant);
        applicationStore.delete(tenant, applicationName);
    }

    private void checkTenant(String tenant) {
        if (globalMetadataService.getTenant(tenant) == null) {
            throw new ResponseStatusException(
                    HttpStatus.NOT_FOUND, "tenant not found"
            );
        }
    }

}
