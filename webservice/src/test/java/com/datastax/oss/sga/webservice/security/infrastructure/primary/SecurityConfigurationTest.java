package com.datastax.oss.sga.webservice.security.infrastructure.primary;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import com.datastax.oss.sga.api.storage.ApplicationStore;
import com.datastax.oss.sga.webservice.application.ApplicationService;
import com.datastax.oss.sga.webservice.common.GlobalMetadataService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest(properties = {
        "application.security.enabled=true",
        "application.security.token.secret-key=jDdra78Vo1+RVMGY2easnWe0sAFrEa2581ra5YMotbE=",
        "application.security.token.auth-claim=iss",
        "application.security.token.admin-roles=testrole"})
@AutoConfigureMockMvc
@DirtiesContext
class SecurityConfigurationTest {


    @Autowired
    MockMvc mockMvc;

    @MockBean
    GlobalMetadataService globalMetadataService;
    @MockBean
    ApplicationStore applicationStore;

    @Test
    void shouldBeAuthorized() throws Exception {
        // Token with "iss": "testrole"
        String token = "eyJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJ0ZXN0cm9sZSJ9.Y6VOsE3vw4zOuRnG_WtVGWn25lgwNGkY5VrRpXR9SVI";
        mockMvc.perform(put("/api/tenants/security-configuration-resource").header("Authorization", "Bearer " + token))
                .andExpect(status().isOk());
    }

    @Test
    void shouldBeForbiddenIfTokenIsInvalid() throws Exception {
        String token = "invalid";
        mockMvc
                .perform(put("/api/tenants/security-configuration-resource").header("Authorization", "Bearer " + token))
                .andExpect(status().isForbidden());
    }

    @Test
    void shouldBeForbiddenIfNotInAdminRole() throws Exception {
        // Token with "iss": "notadmin"
        String token = "eyJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJub3RhZG1pbiJ9.SMRG0RwT4O9XzOOIPhOV2K7TdwDJI4EDNNFruN_3qtc";
        mockMvc
                .perform(put("/api/tenants/security-configuration-resource").header("Authorization", "Bearer " + token))
                .andExpect(status().isForbidden());
    }

}