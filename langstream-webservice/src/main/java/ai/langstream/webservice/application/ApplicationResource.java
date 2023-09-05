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
package ai.langstream.webservice.application;

import ai.langstream.api.codestorage.CodeStorageException;
import ai.langstream.api.model.ApplicationSpecs;
import ai.langstream.api.model.StoredApplication;
import ai.langstream.api.storage.ApplicationStore;
import ai.langstream.api.webservice.application.ApplicationCodeInfo;
import ai.langstream.api.webservice.application.ApplicationDescription;
import ai.langstream.impl.parser.ModelBuilder;
import ai.langstream.webservice.security.infrastructure.primary.TokenAuthFilter;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import net.lingala.zip4j.ZipFile;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

@RestController
@Tag(name = "applications")
@RequestMapping("/api/applications")
@Slf4j
@AllArgsConstructor
public class ApplicationResource {

    ApplicationService applicationService;
    CodeStorageService codeStorageService;

    private final ScheduledExecutorService logsHeartbeatThreadPool =
            Executors.newSingleThreadScheduledExecutor(
                    new BasicThreadFactory.Builder().namingPattern("app-logs-hb-%d").build());
    private final ExecutorService logsThreadPool =
            Executors.newCachedThreadPool(
                    new BasicThreadFactory.Builder().namingPattern("app-logs-%d").build());

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
    @Operation(summary = "Get all applications")
    Collection<ApplicationDescription> getApplications(
            Authentication authentication, @NotBlank @PathVariable("tenant") String tenant) {
        performAuthorization(authentication, tenant);
        return applicationService.getAllApplications(tenant).values().stream()
                .map(
                        app ->
                                new ApplicationDescription(
                                        app.getApplicationId(), app.getInstance(), app.getStatus()))
                .toList();
    }

    @PostMapping(value = "/{tenant}/{id}", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    @Operation(summary = "Create and deploy an application")
    void deployApplication(
            Authentication authentication,
            @NotBlank @PathVariable("tenant") String tenant,
            @NotBlank @PathVariable("id") String applicationId,
            @RequestParam("app") MultipartFile appFile,
            @RequestParam String instance,
            @RequestParam Optional<String> secrets)
            throws Exception {
        performAuthorization(authentication, tenant);
        final ParsedApplication parsedApplication =
                parseApplicationInstance(
                        applicationId,
                        Optional.of(appFile),
                        Optional.of(instance),
                        secrets,
                        tenant);
        applicationService.deployApplication(
                tenant,
                applicationId,
                parsedApplication.getApplication(),
                parsedApplication.getCodeArchiveReference());
    }

    @PatchMapping(value = "/{tenant}/{id}", consumes = "multipart/form-data")
    @Operation(summary = "Update and re-deploy an application")
    void updateApplication(
            Authentication authentication,
            @NotBlank @PathVariable("tenant") String tenant,
            @NotBlank @PathVariable("id") String applicationId,
            @NotNull @RequestParam("app") Optional<MultipartFile> appFile,
            @RequestParam Optional<String> instance,
            @RequestParam Optional<String> secrets)
            throws Exception {
        performAuthorization(authentication, tenant);
        final ParsedApplication parsedApplication =
                parseApplicationInstance(applicationId, appFile, instance, secrets, tenant);
        applicationService.updateApplication(
                tenant,
                applicationId,
                parsedApplication.getApplication(),
                parsedApplication.getCodeArchiveReference());
    }

    @Data
    static class ParsedApplication {
        private ModelBuilder.ApplicationWithPackageInfo application;
        private String codeArchiveReference;
    }

    private ParsedApplication parseApplicationInstance(
            String name,
            Optional<MultipartFile> file,
            Optional<String> instance,
            Optional<String> secrets,
            String tenant)
            throws Exception {
        final ParsedApplication parsedApplication = new ParsedApplication();
        withApplicationZip(
                file,
                (zip, appDirectories) -> {
                    try {
                        final ModelBuilder.ApplicationWithPackageInfo app =
                                ModelBuilder.buildApplicationInstance(
                                        appDirectories,
                                        instance.orElse(null),
                                        secrets.orElse(null));
                        final String codeArchiveReference;
                        if (zip == null) {
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
                    } catch (Exception e) {
                        throw new IllegalArgumentException(e);
                    }
                });
        return parsedApplication;
    }

    private void withApplicationZip(
            Optional<MultipartFile> file, BiConsumer<Path, List<Path>> appDirectoriesConsumer)
            throws Exception {
        if (file.isPresent()) {
            Path tempdir = Files.createTempDirectory("zip-extract");
            final Path tempZip = Files.createTempFile("app", ".zip");
            try {
                file.get().transferTo(tempZip);
                try (ZipFile zipFile = new ZipFile(tempZip.toFile())) {
                    zipFile.extractAll(tempdir.toFile().getAbsolutePath());
                    appDirectoriesConsumer.accept(tempZip, List.of(tempdir));
                }
            } finally {
                tempZip.toFile().delete();

                deleteDirectory(tempdir);
            }
        } else {
            appDirectoriesConsumer.accept(null, List.of());
        }
    }

    private static void deleteDirectory(Path tempdir) throws IOException {
        Files.walk(tempdir)
                .sorted(Comparator.reverseOrder())
                .forEach(
                        path -> {
                            try {
                                log.info("Deleting temporary file {}", path);
                                Files.delete(path); // delete each file or directory
                            } catch (IOException e) {
                                log.info("Cannot delete file {}", path, e);
                            }
                        });
    }

    @DeleteMapping("/{tenant}/{applicationId}")
    @Operation(summary = "Delete application by id")
    void deleteApplication(
            Authentication authentication,
            @NotBlank @PathVariable("tenant") String tenant,
            @NotBlank @PathVariable("applicationId") String applicationId) {
        performAuthorization(authentication, tenant);
        getAppOrThrow(tenant, applicationId);
        applicationService.deleteApplication(tenant, applicationId);
        log.info("Deleted application {}", applicationId);
    }

    @GetMapping("/{tenant}/{applicationId}")
    @Operation(summary = "Get an application by id")
    ApplicationDescription getApplication(
            Authentication authentication,
            @NotBlank @PathVariable("tenant") String tenant,
            @NotBlank @PathVariable("applicationId") String applicationId,
            @RequestParam(value = "stats", required = false) boolean stats) {
        performAuthorization(authentication, tenant);
        final StoredApplication app = getAppWithStatusOrThrow(tenant, applicationId, stats);
        return new ApplicationDescription(
                app.getApplicationId(), app.getInstance(), app.getStatus());
    }

    @GetMapping(
            value = "/{tenant}/{applicationId}/logs",
            produces = MediaType.APPLICATION_NDJSON_VALUE)
    @Operation(summary = "Get application logs by name")
    Flux<String> getApplicationLogs(
            Authentication authentication,
            @NotBlank @PathVariable("tenant") String tenant,
            @NotBlank @PathVariable("applicationId") String applicationId,
            @RequestParam("filter") Optional<List<String>> filterReplicas) {
        performAuthorization(authentication, tenant);
        getAppOrThrow(tenant, applicationId);

        final List<ApplicationStore.PodLogHandler> podLogs =
                applicationService.getPodLogs(
                        tenant,
                        applicationId,
                        new ApplicationStore.LogOptions(filterReplicas.orElse(null)));
        AtomicLong lastSent = new AtomicLong(Long.MAX_VALUE);
        final Consumer<FluxSink<String>> fluxSinkConsumer =
                fluxSink -> {
                    if (podLogs.isEmpty()) {
                        fluxSink.next("No pods found\n");
                        fluxSink.complete();
                        return;
                    }
                    fluxSink.onDispose(
                            () -> podLogs.forEach(ApplicationStore.PodLogHandler::close));
                    logsHeartbeatThreadPool.scheduleWithFixedDelay(
                            () -> {
                                try {
                                    if (lastSent.get() + TimeUnit.SECONDS.toMillis(30)
                                            < System.currentTimeMillis()) {
                                        fluxSink.next("Heartbeat\n");
                                        lastSent.set(System.currentTimeMillis());
                                    }
                                } catch (Throwable e) {
                                }
                            },
                            30,
                            30,
                            TimeUnit.SECONDS);
                    for (ApplicationStore.PodLogHandler podLog : podLogs) {
                        fluxSink.next(
                                "Start receiving log for pod %s\n".formatted(podLog.getPodName()));
                        logsThreadPool.submit(
                                () -> {
                                    try {
                                        final ApplicationStore.LogLineConsumer logLineConsumer =
                                                new ApplicationStore.LogLineConsumer() {
                                                    @Override
                                                    public boolean onLogLine(String line) {
                                                        fluxSink.next(line);
                                                        lastSent.set(System.currentTimeMillis());
                                                        return true;
                                                    }

                                                    @Override
                                                    public void onEnd() {
                                                        lastSent.set(System.currentTimeMillis());
                                                        fluxSink.complete();
                                                    }
                                                };
                                        podLog.start(logLineConsumer);
                                    } catch (Exception e) {
                                        fluxSink.error(e);
                                    }
                                });
                    }
                };
        return Flux.create(fluxSinkConsumer);
    }

    @GetMapping(value = "/{tenant}/{applicationId}/code", produces = "application/zip")
    @Operation(summary = "Get code of an application by id")
    @ResponseBody
    Resource getApplicationCode(
            Authentication authentication,
            @NotBlank @PathVariable("tenant") String tenant,
            @NotBlank @PathVariable("applicationId") String applicationId,
            HttpServletResponse response)
            throws Exception {
        performAuthorization(authentication, tenant);
        final ApplicationSpecs app = getAppOrThrow(tenant, applicationId);
        final String codeArchiveId = app.getCodeArchiveReference();
        return downloadCode(tenant, applicationId, response, codeArchiveId);
    }

    @GetMapping(
            value = "/{tenant}/{applicationId}/code/{codeArchiveReference}",
            produces = "application/zip")
    @Operation(summary = "Get code of an application by id and code archive reference")
    @ResponseBody
    Resource getApplicationCode(
            Authentication authentication,
            @NotBlank @PathVariable("tenant") String tenant,
            @NotBlank @PathVariable("applicationId") String applicationId,
            @NotBlank @PathVariable("codeArchiveReference") String codeArchiveReference,
            HttpServletResponse response)
            throws Exception {
        performAuthorization(authentication, tenant);
        getAppOrThrow(tenant, applicationId);
        return downloadCode(tenant, applicationId, response, codeArchiveReference);
    }

    @GetMapping(
            value = "/{tenant}/{applicationId}/code/{codeArchiveReference}/info",
            produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(
            summary = "Get info about the code of an application by id and code archive reference")
    ApplicationCodeInfo getApplicationCodeInfo(
            Authentication authentication,
            @NotBlank @PathVariable("tenant") String tenant,
            @NotBlank @PathVariable("applicationId") String applicationId,
            @NotBlank @PathVariable("codeArchiveReference") String codeArchiveReference)
            throws Exception {
        performAuthorization(authentication, tenant);
        getAppOrThrow(tenant, applicationId);
        return codeStorageService.getApplicationCodeInfo(
                tenant, applicationId, codeArchiveReference);
    }

    private Resource downloadCode(
            String tenant, String applicationId, HttpServletResponse response, String codeArchiveId)
            throws CodeStorageException {

        final byte[] code =
                codeStorageService.downloadApplicationCode(tenant, applicationId, codeArchiveId);
        final String filename = "%s-%s.zip".formatted(tenant, applicationId);

        response.addHeader(
                "Content-Disposition", "attachment; filename=\"%s\"".formatted(filename));
        return new ByteArrayResource(code);
    }

    private StoredApplication getAppWithStatusOrThrow(
            String tenant, String applicationId, boolean queryPods) {
        final StoredApplication app =
                applicationService.getApplication(tenant, applicationId, queryPods);
        if (app == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "application not found");
        }
        return app;
    }

    private ApplicationSpecs getAppOrThrow(String tenant, String applicationId) {
        final ApplicationSpecs app = applicationService.getApplicationSpecs(tenant, applicationId);
        if (app == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "application not found");
        }
        return app;
    }
}
