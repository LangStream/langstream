package com.datastax.oss.sga.webservice.application;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import com.datastax.oss.sga.cli.commands.applications.DeployApplicationCmd;
import com.datastax.oss.sga.impl.storage.LocalStore;
import com.datastax.oss.sga.webservice.config.StorageProperties;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
@Slf4j
class TenantResourceTest {

    @Autowired
    MockMvc mockMvc;

    @TestConfiguration
    public static class TestConfig {

        @Bean
        @Primary
        @SneakyThrows
        public StorageProperties storageProperties() {
            return new StorageProperties(
                    new StorageProperties.AppsStoreProperties("kubernetes", Map.of("namespaceprefix", "sga-")),
                    new StorageProperties.GlobalMetadataStoreProperties("local",
                            Map.of(
                                    LocalStore.LOCAL_BASEDIR,
                                    Files.createTempDirectory("sga-test").toFile().getAbsolutePath()
                            )
                    )
            );
        }

    }


    @Test
    void testTenantCrud() throws Exception {

        mockMvc.perform(put("/api/tenants/my-tenant"))
                        .andExpect(status().isOk());
        mockMvc
                .perform(
                        put("/api/tenants/test")
                )
                .andExpect(status().isOk());

        mockMvc
                .perform(
                        get("/api/tenants/test")
                )
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("test"));

        mockMvc
                .perform(
                        get("/api/tenants")
                )
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.test.name").value("test"));

        mockMvc
                .perform(
                        delete("/api/tenants/test")
                )
                .andExpect(status().isOk());

        mockMvc
                .perform(
                        get("/api/tenants/test")
                )
                .andExpect(status().isNotFound());

    }
}