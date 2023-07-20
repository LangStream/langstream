package com.datastax.oss.sga.webservice.application;

import com.datastax.oss.sga.api.model.Application;
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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.lingala.zip4j.ZipFile;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
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
    Map<String, StoredApplication> getApplications(@NotBlank @PathVariable("tenant") String tenant) {
        return applicationService.getAllApplications(tenant);
    }

    @PutMapping(value = "/{tenant}/{name}", consumes = "multipart/form-data")
    @Operation(summary = "Create and deploy an application")
    void deployApplication(
            @NotBlank @PathVariable("tenant") String tenant,
            @NotBlank @PathVariable("name") String name,
            @NotNull @RequestParam("file") MultipartFile file) throws Exception {
        final Map.Entry<Application, String> instance = parseApplicationInstance(name, file, tenant);
        log.info("Parsed application instance code:{} application:{}", instance.getValue(), instance.getKey());
        applicationService.deployApplication(tenant, name, instance.getKey(), instance.getValue());
    }

    private Map.Entry<Application, String> parseApplicationInstance(String name, MultipartFile file, String tenant)
            throws Exception {
        Path tempdir = Files.createTempDirectory("zip-extract");
        final Path tempZip = Files.createTempFile("app", ".zip");
        try {
            file.transferTo(tempZip);
            try (ZipFile zipFile = new ZipFile(tempZip.toFile());) {
                zipFile.extractAll(tempdir.toFile().getAbsolutePath());
                final Application applicationInstance =
                        ModelBuilder.buildApplicationInstance(List.of(tempdir));
                String codeArchiveReference = codeStorageService.deployApplicationCodeStorage(tenant, name,
                        applicationInstance, tempZip);
                log.info("Parsed application {} with code archive {}", name, codeArchiveReference);
                return new AbstractMap.SimpleImmutableEntry<>(applicationInstance, codeArchiveReference);
            }
        } finally {
            tempZip.toFile().delete();

            deleteDirectory(tempdir);
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
    StoredApplication getApplication(@NotBlank @PathVariable("tenant") String tenant,
                                     @NotBlank @PathVariable("name") String name) {
        final StoredApplication app = applicationService.getApplication(tenant, name);
        if (app == null) {
            throw new ResponseStatusException(
                    HttpStatus.NOT_FOUND, "application not found"
            );
        }
        return app;
    }

    @GetMapping(value = "/{tenant}/{name}/logs", produces = MediaType.APPLICATION_NDJSON_VALUE)
    @Operation(summary = "Get application logs by name")
    Flux<String> getApplicationLogs(@NotBlank @PathVariable("tenant") String tenant,
                                    @NotBlank @PathVariable("name") String name,
                                    @RequestParam("filter") Optional<List<String>> filterReplicas) {


        final List<ApplicationStore.PodLogHandler> podLogs =
                applicationService.getPodLogs(tenant, name, new ApplicationStore.LogOptions(filterReplicas.orElse(null)));
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


