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
package ai.langstream.webservice.archetype;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import ai.langstream.impl.k8s.tests.KubeK3sServer;
import ai.langstream.webservice.WebAppTestConfig;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest(properties = {"application.archetypes.path=src/main/docker/jib/app/archetypes"})
@AutoConfigureMockMvc
@Slf4j
@Import(WebAppTestConfig.class)
@DirtiesContext
class ArchetypeResourceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Autowired MockMvc mockMvc;

    @RegisterExtension static final KubeK3sServer k3s = new KubeK3sServer(true);

    @Test
    void testArchetypesMetadata() throws Exception {
        mockMvc.perform(put("/api/tenants/my-tenant")).andExpect(status().isOk());
        mockMvc.perform(get("/api/archetypes/my-tenant"))
                .andExpect(status().isOk())
                .andExpect(
                        result -> {
                            List<Map<String, Object>> list =
                                    MAPPER.readValue(
                                            result.getResponse().getContentAsString(),
                                            new TypeReference<List<Map<String, Object>>>() {});
                            assertEquals(1, list.size());
                            assertEquals("website-qa-chatbot", list.get(0).get("id"));
                        });
        mockMvc.perform(get("/api/archetypes/my-tenant/not-exists"))
                .andExpect(status().isNotFound());

        mockMvc.perform(get("/api/archetypes/my-tenant/website-qa-chatbot"))
                .andExpect(status().isOk())
                .andExpect(
                        result -> {
                            log.info("Result {}", result.getResponse().getContentAsString());
                        });
    }
}
