package com.datastax.oss.sga.webservice.application;

import com.datastax.oss.sga.api.codestorage.CodeStorage;
import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.StoredApplication;
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

    private CodeStorage codeStorage;

    public ApplicationService(
            GlobalMetadataService globalMetadataService,
            ApplicationStore store) {
        this.globalMetadataService = globalMetadataService;
        this.applicationStore = store;
    }


    @SneakyThrows
    public Map<String, StoredApplication> getAllApplications(String tenant) {
        checkTenant(tenant);
        return applicationStore.list(tenant);
    }

    @SneakyThrows
    public void deployApplication(String tenant, String applicationId, Application applicationInstance,
                                  String codeArchiveReference) {
        checkTenant(tenant);
        applicationStore.put(tenant, applicationId, applicationInstance, codeArchiveReference);
    }

    @SneakyThrows
    public StoredApplication getApplication(String tenant, String applicationId) {
        checkTenant(tenant);
        return applicationStore.get(tenant, applicationId);
    }

    @SneakyThrows
    public void deleteApplication(String tenant, String applicationId) {
        checkTenant(tenant);
        applicationStore.delete(tenant, applicationId);
    }

    private void checkTenant(String tenant) {
        if (globalMetadataService.getTenant(tenant) == null) {
            throw new ResponseStatusException(
                    HttpStatus.NOT_FOUND, "tenant not found"
            );
        }
    }

}
