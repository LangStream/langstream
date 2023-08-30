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

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import ai.langstream.impl.k8s.tests.KubeK3sServer;
import ai.langstream.webservice.WebAppTestConfig;
import lombok.extern.slf4j.Slf4j;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest(properties = {"application.tenants.max-total-resource-units-limit=8"})
@AutoConfigureMockMvc
@Slf4j
@Import(WebAppTestConfig.class)
@DirtiesContext
class TenantResourceTest {

    @Autowired MockMvc mockMvc;

    @RegisterExtension static final KubeK3sServer k3s = new KubeK3sServer(true);

    @Test
    void testTenantCrud() throws Exception {
        mockMvc.perform(put("/api/tenants/my-tenant")).andExpect(status().isOk());
        mockMvc.perform(put("/api/tenants/test")).andExpect(status().isOk());

        mockMvc.perform(
                        patch("/api/tenants/test")
                                .contentType("application/json")
                                .content("{\"maxTotalResourceUnits\":1}"))
                .andExpect(status().isOk());
        mockMvc.perform(
                        patch("/api/tenants/test")
                                .contentType("application/json")
                                .content("{\"maxTotalResourceUnits\":0}"))
                .andExpect(status().isOk());

        mockMvc.perform(
                        patch("/api/tenants/test")
                                .contentType("application/json")
                                .content("{\"maxTotalResourceUnits\":8}"))
                .andExpect(status().isOk());
        mockMvc.perform(
                        patch("/api/tenants/test")
                                .contentType("application/json")
                                .content("{\"maxTotalResourceUnits\":-111}"))
                .andExpect(status().isBadRequest());

        mockMvc.perform(
                        patch("/api/tenants/test")
                                .contentType("application/json")
                                .content("{\"maxTotalResourceUnits\":9}"))
                .andExpect(status().isBadRequest());

        mockMvc.perform(
                        patch("/api/tenants/test2")
                                .contentType("application/json")
                                .content("{\"maxTotalResourceUnits\":0}"))
                .andExpect(status().isNotFound());

        mockMvc.perform(
                        post("/api/tenants/test")
                                .contentType("application/json")
                                .content("{\"maxTotalResourceUnits\":0}"))
                .andExpect(status().isConflict());

        mockMvc.perform(
                        post("/api/tenants/test2")
                                .contentType("application/json")
                                .content("{\"maxTotalResourceUnits\":1}"))
                .andExpect(status().isOk());
        mockMvc.perform(
                        post("/api/tenants/test3")
                                .contentType("application/json")
                                .content("{\"maxTotalResourceUnits\":0}"))
                .andExpect(status().isOk());
        mockMvc.perform(
                        post("/api/tenants/test4")
                                .contentType("application/json")
                                .content("{\"maxTotalResourceUnits\":-111}"))
                .andExpect(status().isBadRequest());

        mockMvc.perform(get("/api/tenants/test"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("test"));

        mockMvc.perform(get("/api/tenants"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.test.name").value("test"))
                .andExpect(jsonPath("$.test.maxTotalResourceUnits").value(8));

        mockMvc.perform(delete("/api/tenants/test")).andExpect(status().isOk());
        mockMvc.perform(put("/api/tenants/test")).andExpect(status().isInternalServerError());

        mockMvc.perform(get("/api/tenants/test"))
                .andExpect(
                        status().is(
                                        new BaseMatcher<Integer>() {
                                            @Override
                                            public boolean matches(Object o) {

                                                return (int) o == 500 || (int) o == 404;
                                            }

                                            @Override
                                            public void describeTo(Description description) {}
                                        }));
    }
}
