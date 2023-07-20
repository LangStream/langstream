package com.datastax.oss.sga.webservice.application;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import com.datastax.oss.sga.impl.k8s.tests.KubeK3sServer;
import com.datastax.oss.sga.impl.storage.LocalStore;
import com.datastax.oss.sga.webservice.WebAppTestConfig;
import com.datastax.oss.sga.webservice.config.StorageProperties;
import java.nio.file.Files;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
@Slf4j
@Import(WebAppTestConfig.class)
@DirtiesContext
class TenantResourceTest {

    @Autowired
    MockMvc mockMvc;

    @RegisterExtension
    static final KubeK3sServer k3s = new KubeK3sServer(true);


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