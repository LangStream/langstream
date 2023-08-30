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
package ai.langstream.webservice.security.infrastructure.primary;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import ai.langstream.api.storage.ApplicationStore;
import ai.langstream.webservice.common.GlobalMetadataService;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

@SpringBootTest(
        properties = {
            "application.security.enabled=true",
            "application.security.token.secret-key=jDdra78Vo1+RVMGY2easnWe0sAFrEa2581ra5YMotbE=",
            "application.security.token.auth-claim=iss",
            "application.security.token.admin-roles=testrole"
        })
@AutoConfigureMockMvc
@DirtiesContext
class SecurityConfigurationTest {

    protected static final String ROLE_NOTADMIN =
            "eyJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJub3RhZG1pbiJ9.SMRG0RwT4O9XzOOIPhOV2K7TdwDJI4EDNNFruN_3qtc";
    protected static final String ROLE_TESTROLE =
            "eyJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJ0ZXN0cm9sZSJ9.Y6VOsE3vw4zOuRnG_WtVGWn25lgwNGkY5VrRpXR9SVI";
    @Autowired MockMvc mockMvc;

    @MockBean GlobalMetadataService globalMetadataService;
    @MockBean ApplicationStore applicationStore;

    @Test
    void shouldBeAuthorized() throws Exception {
        // Token with "iss": "testrole"
        String token = ROLE_TESTROLE;
        mockMvc.perform(
                        put("/api/tenants/security-configuration-resource")
                                .header("Authorization", "Bearer " + token))
                .andExpect(status().isOk());
    }

    @Test
    void shouldBeForbiddenIfTokenIsInvalid() throws Exception {
        String token = "invalid";
        mockMvc.perform(
                        put("/api/tenants/security-configuration-resource")
                                .header("Authorization", "Bearer " + token))
                .andExpect(status().isForbidden());
    }

    @Test
    void shouldBeForbiddenIfNotInAdminRole() throws Exception {
        // Token with "iss": "notadmin"
        String token = ROLE_NOTADMIN;
        mockMvc.perform(
                        put("/api/tenants/security-configuration-resource")
                                .header("Authorization", "Bearer " + token))
                .andExpect(status().isForbidden());
    }

    @Test
    void shouldBeForbiddenIfNotSameTenant() throws Exception {
        final BaseMatcher<Integer> authorizedMatcher =
                new BaseMatcher<>() {
                    @Override
                    public boolean matches(Object o) {
                        return (int) o != HttpStatus.FORBIDDEN.value();
                    }

                    @Override
                    public void describeTo(Description description) {}
                };

        // Token with "iss": "notadmin"
        String token = ROLE_NOTADMIN;
        List<Map.Entry<HttpMethod, String>> requests = new ArrayList<>();
        requests.add(Map.entry(HttpMethod.GET, "/api/applications/{tenant}"));
        requests.add(Map.entry(HttpMethod.GET, "/api/applications/{tenant}/app"));
        requests.add(Map.entry(HttpMethod.GET, "/api/applications/{tenant}/app/logs"));
        requests.add(Map.entry(HttpMethod.GET, "/api/applications/{tenant}/app/code"));
        requests.add(Map.entry(HttpMethod.GET, "/api/applications/{tenant}/app/code/code-id-1"));
        requests.add(Map.entry(HttpMethod.DELETE, "/api/applications/{tenant}/app"));

        for (Map.Entry<HttpMethod, String> entry : requests) {
            System.out.println("testing " + entry.getKey() + " " + entry.getValue());
            mockMvc.perform(
                            MockMvcRequestBuilders.request(
                                            entry.getKey(),
                                            entry.getValue().replace("{tenant}", "notadmin"))
                                    .header("Authorization", "Bearer " + token))
                    .andExpect(status().is(authorizedMatcher));

            mockMvc.perform(
                            MockMvcRequestBuilders.request(
                                            entry.getKey(),
                                            entry.getValue().replace("{tenant}", "another-tenant"))
                                    .header("Authorization", "Bearer " + token))
                    .andExpect(status().isForbidden());

            mockMvc.perform(
                            MockMvcRequestBuilders.request(
                                            entry.getKey(),
                                            entry.getValue().replace("{tenant}", "notadmin"))
                                    .header("Authorization", "Bearer " + ROLE_TESTROLE))
                    .andExpect(status().is(authorizedMatcher));
        }
    }
}
