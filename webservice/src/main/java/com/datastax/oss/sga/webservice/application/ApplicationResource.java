package com.datastax.oss.sga.webservice.application;

import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeRegistry;
import com.datastax.oss.sga.api.runtime.PhysicalApplicationInstance;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.impl.deploy.ApplicationDeployer;
import com.datastax.oss.sga.impl.parser.ModelBuilder;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import net.lingala.zip4j.ZipFile;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
@Tag(name = "applications")
@RequestMapping("/api/applications")
@Slf4j
public class ApplicationResource {

    ApplicationDeployer<PhysicalApplicationInstance> deployer = ApplicationDeployer
            .builder()
            .pluginsRegistry(new PluginsRegistry())
            .registry(new ClusterRuntimeRegistry())
            .build();

    static final Map<String, ApplicationInstance> apps = new HashMap<>();

    @GetMapping("")
    @Operation(summary = "Get all applications")
    Map<String, ApplicationInstance> getApplications() {
        return apps;
    }

    @PutMapping(value = "/{name}", consumes = "multipart/form-data")
    @Operation(summary = "Get all applications")
    void deployApplication(@NotBlank String name,
                        @NotNull @RequestParam("file") MultipartFile file) throws Exception {
        createApplicationFromZip(name, file);
    }

    private void createApplicationFromZip(String name, MultipartFile file) throws Exception {
        Path tempdir = Files.createTempDirectory("zip-extract");
        final Path tempZip = Files.createTempFile("app", ".zip");
        try {
            file.transferTo(tempZip);
            try (ZipFile zipFile = new ZipFile(tempZip.toFile());) {
                zipFile.extractAll(tempdir.toFile().getAbsolutePath());
                final ApplicationInstance applicationInstance =
                        ModelBuilder.buildApplicationInstance(List.of(tempdir));
                apps.put(name, applicationInstance);
                final PhysicalApplicationInstance implementation = deployer.createImplementation(applicationInstance);
                deployer.deploy(applicationInstance, implementation);
                log.info("Deployed application {} of {}: {}", name, implementation.getClass().getSimpleName(),
                        applicationInstance);
            }
        } finally {
            tempdir.toFile().delete();
            tempZip.toFile().delete();
        }
    }
}