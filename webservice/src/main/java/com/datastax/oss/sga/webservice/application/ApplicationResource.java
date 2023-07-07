package com.datastax.oss.sga.webservice.application;

import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.model.StoredApplicationInstance;
import com.datastax.oss.sga.impl.parser.ModelBuilder;
import com.datastax.oss.sga.webservice.common.GlobalMetadataService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.lingala.zip4j.ZipFile;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

@RestController
@Tag(name = "applications")
@RequestMapping("/api/applications")
@Slf4j
@AllArgsConstructor
public class ApplicationResource {

    ApplicationService applicationService;

    @GetMapping("/{tenant}")
    @Operation(summary = "Get all applications")
    Map<String, StoredApplicationInstance> getApplications(@NotBlank @PathVariable("tenant") String tenant) {
        return applicationService.getAllApplications(tenant);
    }



    @PutMapping(value = "/{tenant}/{name}", consumes = "multipart/form-data")
    @Operation(summary = "Create and deploy an application")
    void deployApplication(
            @NotBlank @PathVariable("tenant") String tenant,
            @NotBlank @PathVariable("name") String name,
            @NotNull @RequestParam("file") MultipartFile file) throws Exception {
        final ApplicationInstance instance = parseApplicationInstance(name, file);
        applicationService.deployApplication(tenant, name, instance);
    }

    private ApplicationInstance parseApplicationInstance(String name, MultipartFile file) throws Exception {
        Path tempdir = Files.createTempDirectory("zip-extract");
        final Path tempZip = Files.createTempFile("app", ".zip");
        try {
            file.transferTo(tempZip);
            try (ZipFile zipFile = new ZipFile(tempZip.toFile());) {
                zipFile.extractAll(tempdir.toFile().getAbsolutePath());
                final ApplicationInstance applicationInstance =
                        ModelBuilder.buildApplicationInstance(List.of(tempdir));
                return applicationInstance;

            }
        } finally {
            tempdir.toFile().delete();
            tempZip.toFile().delete();
        }
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
    StoredApplicationInstance getApplication(@NotBlank @PathVariable("tenant") String tenant,
                                             @NotBlank @PathVariable("name") String name) {
        final StoredApplicationInstance app = applicationService.getApplication(tenant, name);
        if (app == null) {
            throw new ResponseStatusException(
                    HttpStatus.NOT_FOUND, "application not found"
            );
        }
        return app;
    }

}
