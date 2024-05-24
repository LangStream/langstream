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
package ai.langstream.webservice.archetype;

import ai.langstream.api.archetype.ArchetypeDefinition;
import ai.langstream.api.model.Application;
import ai.langstream.api.webservice.application.ApplicationDescription;
import ai.langstream.impl.common.ApplicationPlaceholderResolver;
import ai.langstream.impl.parser.ModelBuilder;
import ai.langstream.webservice.application.ApplicationService;
import ai.langstream.webservice.application.CodeStorageService;
import ai.langstream.webservice.security.infrastructure.primary.TokenAuthFilter;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.constraints.NotBlank;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@Tag(name = "archetypes")
@RequestMapping("/api/archetypes")
@Slf4j
@AllArgsConstructor
public class ArchetypeResource {

    ArchetypeService archetypeService;
    ApplicationService applicationService;
    CodeStorageService codeStorageService;

    private void performAuthorization(Authentication authentication, final String tenant) {
        if (authentication == null) {
            return;
        }
        if (!authentication.isAuthenticated()) {
            throw new IllegalStateException();
        }
        if (authentication.getAuthorities() != null) {
            final GrantedAuthority grantedAuthority =
                    authentication.getAuthorities().stream()
                            .filter(
                                    authority ->
                                            authority
                                                    .getAuthority()
                                                    .equals(TokenAuthFilter.ROLE_ADMIN))
                            .findFirst()
                            .orElse(null);
            if (grantedAuthority != null) {
                return;
            }
        }
        if (authentication.getPrincipal() == null) {
            throw new IllegalStateException();
        }
        final String principal = authentication.getPrincipal().toString();
        if (tenant.equals(principal)) {
            return;
        }
        throw new ResponseStatusException(HttpStatus.FORBIDDEN);
    }

    @GetMapping("/{tenant}")
    @Operation(summary = "Get all archetypes")
    List<ArchetypeBasicInfo> getArchetypes(
            Authentication authentication, @NotBlank @PathVariable("tenant") String tenant) {
        performAuthorization(authentication, tenant);
        return archetypeService.getAllArchetypesBasicInfo(tenant);
    }

    @GetMapping("/{tenant}/{id}")
    @Operation(summary = "Get metadata about an archetype")
    ArchetypeDefinition getArchetype(
            Authentication authentication,
            @NotBlank @PathVariable("tenant") String tenant,
            @NotBlank @PathVariable("id") String id) {
        performAuthorization(authentication, tenant);
        ArchetypeDefinition result = archetypeService.getArchetype(tenant, id);
        if (result == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        }
        return result;
    }

    @Data
    static class ParsedApplication {
        private ModelBuilder.ApplicationWithPackageInfo application;
        private String codeArchiveReference;
    }

    @PostMapping(
            value = "/{tenant}/{idarchetype}/applications/{idapplication}",
            consumes = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "Create and deploy an application from an archetype")
    ApplicationDescription.ApplicationDefinition deployApplication(
            Authentication authentication,
            @NotBlank @PathVariable("tenant") String tenant,
            @NotBlank @PathVariable("idapplication") String applicationId,
            @NotBlank @PathVariable("idarchetype") String idarchetype,
            @RequestBody Map<String, Object> applicationParameters,
            @RequestParam(value = "dry-run", required = false) boolean dryRun)
            throws Exception {
        performAuthorization(authentication, tenant);

        ArchetypeDefinition archetype = archetypeService.getArchetype(tenant, idarchetype);
        if (archetype == null) {
            log.info("Archetype {} not found", idarchetype);
            throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        }

        final ParsedApplication parsedApplication =
                buildApplicationFromArchetype(
                        applicationId, idarchetype, applicationParameters, tenant, dryRun);
        final Application application;
        if (dryRun) {
            application =
                    ApplicationPlaceholderResolver.resolvePlaceholders(
                            parsedApplication.getApplication().getApplication());

        } else {
            applicationService.deployApplication(
                    tenant,
                    applicationId,
                    parsedApplication.getApplication(),
                    parsedApplication.getCodeArchiveReference(),
                    false);
            application = parsedApplication.getApplication().getApplication();
        }
        return new ApplicationDescription.ApplicationDefinition(application);
    }

    private ParsedApplication buildApplicationFromArchetype(
            String name,
            String archetypeId,
            Map<String, Object> applicationParameters,
            String tenant,
            boolean dryRun)
            throws Exception {
        final ParsedApplication parsedApplication = new ParsedApplication();
        Path archetypePath = archetypeService.getArchetypePath(archetypeId);
        final ModelBuilder.ApplicationWithPackageInfo app =
                ModelBuilder.buildApplicationInstanceFromArchetype(
                        archetypePath, applicationParameters);
        final Path zip = archetypeService.buildArchetypeZip(archetypeId);

        final String codeArchiveReference;
        if (dryRun) {
            codeArchiveReference = null;
        } else {
            codeArchiveReference =
                    codeStorageService.deployApplicationCodeStorage(
                            tenant,
                            name,
                            zip,
                            app.getPyBinariesDigest(),
                            app.getJavaBinariesDigest());
        }
        log.info(
                "Parsed application {} {} with code archive {}",
                name,
                app.getApplication(),
                codeArchiveReference);
        parsedApplication.setApplication(app);
        parsedApplication.setCodeArchiveReference(codeArchiveReference);

        return parsedApplication;
    }
}
