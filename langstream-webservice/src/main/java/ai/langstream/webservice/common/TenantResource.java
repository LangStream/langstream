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

import ai.langstream.api.webservice.tenant.CreateTenantRequest;
import ai.langstream.api.webservice.tenant.TenantConfiguration;
import ai.langstream.api.webservice.tenant.UpdateTenantRequest;
import ai.langstream.impl.storage.tenants.TenantException;
import ai.langstream.webservice.application.ApplicationService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.constraints.NotBlank;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@Tag(name = "tenants")
@RequestMapping("/api/tenants")
@Slf4j
public class TenantResource {

    GlobalMetadataService globalMetadataService;
    ApplicationService applicationService;

    public TenantResource(
            GlobalMetadataService globalMetadataService, ApplicationService applicationService) {
        this.globalMetadataService = globalMetadataService;
        this.applicationService = applicationService;
    }

    @GetMapping("")
    @Operation(summary = "List tenants")
    Map<String, TenantConfiguration> getTenants() {
        return globalMetadataService.listTenants();
    }

    @GetMapping("/{tenant}")
    @Operation(summary = "Get tenant metadata")
    TenantConfiguration getTenant(@NotBlank @PathVariable("tenant") String tenant) {
        final TenantConfiguration configuration = globalMetadataService.getTenant(tenant);
        if (configuration == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "tenant not found");
        }
        return configuration;
    }

    @PostMapping(value = "/{tenant}", consumes = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "Create a tenant")
    void createTenant(
            @NotBlank @PathVariable("tenant") String tenant,
            @RequestBody CreateTenantRequest createTenantRequest)
            throws TenantException {
        try {
            globalMetadataService.createTenant(tenant, createTenantRequest);
        } catch (TenantException e) {
            switch (e.getType()) {
                case AlreadyExists:
                    throw new ResponseStatusException(HttpStatus.CONFLICT, "tenant already exists");
                default:
                    throw e;
            }
        }
    }

    @PatchMapping(value = "/{tenant}", consumes = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "Update a tenant")
    void createTenant(
            @NotBlank @PathVariable("tenant") String tenant,
            @RequestBody UpdateTenantRequest updateTenantRequest)
            throws TenantException {
        try {
            globalMetadataService.updateTenant(tenant, updateTenantRequest);
        } catch (TenantException e) {
            switch (e.getType()) {
                case NotFound:
                    throw new ResponseStatusException(HttpStatus.NOT_FOUND, "tenant not found");
                default:
                    throw e;
            }
        }
    }

    @PutMapping(value = "/{tenant}")
    @Operation(summary = "Create or update a tenant")
    void putTenant(@NotBlank @PathVariable("tenant") String tenant) {
        globalMetadataService.putTenant(tenant, TenantConfiguration.builder().name(tenant).build());
    }

    @DeleteMapping("/{tenant}")
    @Operation(summary = "Delete tenant")
    void deleteTenant(@NotBlank @PathVariable("tenant") String tenant) throws TenantException {
        applicationService
                .getAllApplications(tenant)
                .keySet()
                .forEach(
                        app -> {
                            try {
                                applicationService.deleteApplication(tenant, app, true);
                            } catch (Exception e) {
                                log.error(
                                        "Error deleting application {} for tenant {}",
                                        app,
                                        tenant,
                                        e);
                                throw new RuntimeException(e);
                            }
                        });
        globalMetadataService.deleteTenant(tenant);
        log.info("Deleted tenant {}", tenant);
    }
}
