/**
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
package com.datastax.oss.sga.webservice.application;

import com.datastax.oss.sga.api.model.StoredApplication;
import com.datastax.oss.sga.api.storage.ApplicationStore;
import com.datastax.oss.sga.impl.parser.ModelBuilder;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import net.lingala.zip4j.ZipFile;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Schedulers;

@RestController
@Tag(name = "applications")
@RequestMapping("/api/applications")
@Slf4j
@AllArgsConstructor
public class ApplicationResource {

    ApplicationService applicationService;
    CodeStorageService codeStorageService;

    private final ExecutorService logsThreadPool = Executors.newCachedThreadPool();

    @GetMapping("/{tenant}")
    @Operation(summary = "Get all applications")
    Collection<ApplicationDescription> getApplications(@NotBlank @PathVariable("tenant") String tenant) {
        return applicationService.getAllApplications(tenant).values().stream()
                .map(app -> new ApplicationDescription(app.getApplicationId(), app.getInstance(), app.getStatus()))
                .toList();
    }

    @PostMapping(value = "/{tenant}/{id}", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    @Operation(summary = "Create and deploy an application")
    void deployApplication(
            @NotBlank @PathVariable("tenant") String tenant,
            @NotBlank @PathVariable("id") String applicationId,
            @RequestParam("app") MultipartFile appFile,
            @RequestParam String instance,
            @RequestParam Optional<String> secrets) throws Exception {
        final ParsedApplication parsedApplication =
                parseApplicationInstance(applicationId,
                        Optional.of(appFile),
                        Optional.of(instance),
                        secrets,
                        tenant);
        applicationService.deployApplication(tenant, applicationId, parsedApplication.getApplication(),
                parsedApplication.getCodeArchiveReference());
    }

    @PatchMapping(value = "/{tenant}/{id}", consumes = "multipart/form-data")
    @Operation(summary = "Update and re-deploy an application")
    void updateApplication(
            @NotBlank @PathVariable("tenant") String tenant,
            @NotBlank @PathVariable("id") String applicationId,
            @NotNull @RequestParam("app") Optional<MultipartFile> appFile,
            @RequestParam Optional<String> instance,
            @RequestParam Optional<String> secrets) throws Exception {
        final ParsedApplication parsedApplication = parseApplicationInstance(applicationId,
                appFile,
                instance, secrets, tenant);
        applicationService.updateApplication(tenant, applicationId,
                parsedApplication.getApplication(),
                parsedApplication.getCodeArchiveReference());
    }


    @Data
    static class ParsedApplication {
        private ModelBuilder.ApplicationWithPackageInfo application;
        private String codeArchiveReference;
    }

    private ParsedApplication parseApplicationInstance(String name,
                                                       Optional<MultipartFile> file,
                                                       Optional<String> instance,
                                                       Optional<String> secrets,
                                                       String tenant)
            throws Exception {
        final ParsedApplication parsedApplication = new ParsedApplication();
        withApplicationZip(file, (zip, appDirectories) -> {
            try {
                final ModelBuilder.ApplicationWithPackageInfo app =
                        ModelBuilder.buildApplicationInstance(appDirectories, instance.orElse(null),
                                secrets.orElse(null));
                final String codeArchiveReference;
                if (zip == null) {
                    codeArchiveReference = null;
                } else {
                    codeArchiveReference = codeStorageService.deployApplicationCodeStorage(tenant, name, zip);

                }
                log.info("Parsed application {} {} with code archive {}", name, app.getApplication(),
                        codeArchiveReference);
                parsedApplication.setApplication(app);
                parsedApplication.setCodeArchiveReference(codeArchiveReference);
            } catch (Exception e) {
                throw new IllegalArgumentException(e);
            }
        });
        return parsedApplication;
    }

    private void withApplicationZip(Optional<MultipartFile> file, BiConsumer<Path, List<Path>> appDirectoriesConsumer)
            throws Exception {
        if (file.isPresent()) {
            Path tempdir = Files.createTempDirectory("zip-extract");
            final Path tempZip = Files.createTempFile("app", ".zip");
            try {
                file.get().transferTo(tempZip);
                try (ZipFile zipFile = new ZipFile(tempZip.toFile());) {
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
        Files
                .walk(tempdir)
                .sorted(Comparator.reverseOrder())
                .forEach(path -> {
                    try {
                        log.info("Deleting temporary file {}", path);
                        Files.delete(path);  //delete each file or directory
                    } catch (IOException e) {
                        log.info("Cannot delete file {}", path, e);
                    }
                });
    }

    @DeleteMapping("/{tenant}/{name}")
    @Operation(summary = "Delete application by name")
    void deleteApplication(@NotBlank @PathVariable("tenant") String tenant,
                           @NotBlank @PathVariable("name") String name) {
        applicationService.deleteApplication(tenant, name);
        log.info("Deleted application {}", name);
    }

    @GetMapping("/{tenant}/{name}")
    @Operation(summary = "Get an application by name")
    ApplicationDescription getApplication(@NotBlank @PathVariable("tenant") String tenant,
                                     @NotBlank @PathVariable("name") String name) {
        final StoredApplication app = applicationService.getApplication(tenant, name);
        if (app == null) {
            throw new ResponseStatusException(
                    HttpStatus.NOT_FOUND, "application not found"
            );
        }
        return new ApplicationDescription(app.getApplicationId(), app.getInstance(), app.getStatus());
    }


    @GetMapping(value = "/{tenant}/{name}/logs", produces = MediaType.APPLICATION_NDJSON_VALUE)
    @Operation(summary = "Get application logs by name")
    Flux<String> getApplicationLogs(@NotBlank @PathVariable("tenant") String tenant,
                                    @NotBlank @PathVariable("name") String name,
                                    @RequestParam("filter") Optional<List<String>> filterReplicas) {


        final List<ApplicationStore.PodLogHandler> podLogs =
                applicationService.getPodLogs(tenant, name,
                        new ApplicationStore.LogOptions(filterReplicas.orElse(null)));
        if (podLogs.isEmpty()) {
            return Flux.empty();
        }
        return Flux.create((Consumer<FluxSink<String>>) fluxSink -> {
            for (ApplicationStore.PodLogHandler podLog : podLogs) {
                logsThreadPool.submit(() -> {
                    try {
                        podLog.start(line -> {
                            fluxSink.next(line);
                            return true;
                        });
                    } catch (Exception e) {
                        fluxSink.error(e);
                    }
                });
            }
        }).subscribeOn(Schedulers.fromExecutor(logsThreadPool));
    }
}


