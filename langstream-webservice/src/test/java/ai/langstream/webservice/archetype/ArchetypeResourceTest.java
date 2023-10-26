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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import ai.langstream.api.model.Secrets;
import ai.langstream.api.storage.ApplicationStore;
import ai.langstream.impl.k8s.tests.KubeK3sServer;
import ai.langstream.webservice.WebAppTestConfig;
import ai.langstream.webservice.application.CodeStorageService;
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

@SpringBootTest(properties = {"application.archetypes.path=src/test/archetypes"})
@AutoConfigureMockMvc
@Slf4j
@Import(WebAppTestConfig.class)
@DirtiesContext
class ArchetypeResourceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Autowired MockMvc mockMvc;

    @RegisterExtension static final KubeK3sServer k3s = new KubeK3sServer(true);

    @Autowired ApplicationStore applicationStore;

    @Autowired CodeStorageService codeStorageService;

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
                            assertEquals("simple", list.get(0).get("id"));
                        });
        mockMvc.perform(get("/api/archetypes/my-tenant/not-exists"))
                .andExpect(status().isNotFound());

        mockMvc.perform(get("/api/archetypes/my-tenant/simple"))
                .andExpect(status().isOk())
                .andExpect(
                        result -> {
                            log.info("Result {}", result.getResponse().getContentAsString());
                            assertEquals(
                                    """
                                             {
                                               "archetype" : {
                                                 "id" : "simple",
                                                 "title" : "Simple",
                                                 "labels" : null,
                                                 "description" : null,
                                                 "icon" : null,
                                                 "sections" : [ {
                                                   "title" : "Section 1",
                                                   "description" : "Xxxxx",
                                                   "parameters" : [ {
                                                     "default" : null,
                                                     "name" : "s1",
                                                     "label" : null,
                                                     "description" : null,
                                                     "type" : null,
                                                     "subtype" : null,
                                                     "binding" : "globals.string-value",
                                                     "required" : false
                                                   }, {
                                                     "default" : null,
                                                     "name" : "i1",
                                                     "label" : null,
                                                     "description" : null,
                                                     "type" : null,
                                                     "subtype" : null,
                                                     "binding" : "globals.input-value",
                                                     "required" : false
                                                   }, {
                                                     "default" : null,
                                                     "name" : "r1",
                                                     "label" : null,
                                                     "description" : null,
                                                     "type" : null,
                                                     "subtype" : null,
                                                     "binding" : "globals.int-value",
                                                     "required" : false
                                                   }, {
                                                     "default" : null,
                                                     "name" : "m1",
                                                     "label" : null,
                                                     "description" : null,
                                                     "type" : null,
                                                     "subtype" : null,
                                                     "binding" : "globals.map-value",
                                                     "required" : false
                                                   }, {
                                                     "default" : null,
                                                     "name" : "l1",
                                                     "label" : null,
                                                     "description" : null,
                                                     "type" : null,
                                                     "subtype" : null,
                                                     "binding" : "globals.list-value",
                                                     "required" : false
                                                   }, {
                                                     "default" : null,
                                                     "name" : "m2",
                                                     "label" : null,
                                                     "description" : null,
                                                     "type" : null,
                                                     "subtype" : null,
                                                     "binding" : "globals.nested-map.key2.key2-1",
                                                     "required" : false
                                                   } ]
                                                 }, {
                                                   "title" : "Section 2",
                                                   "description" : "Xxxxx",
                                                   "parameters" : [ {
                                                     "default" : null,
                                                     "name" : "s2",
                                                     "label" : null,
                                                     "description" : null,
                                                     "type" : null,
                                                     "subtype" : null,
                                                     "binding" : "secrets.open-ai.foo",
                                                     "required" : false
                                                   }, {
                                                     "default" : null,
                                                     "name" : "i2",
                                                     "label" : null,
                                                     "description" : null,
                                                     "type" : null,
                                                     "subtype" : null,
                                                     "binding" : "secrets.open-ai.foo-int",
                                                     "required" : false
                                                   }, {
                                                     "default" : null,
                                                     "name" : "k2",
                                                     "label" : null,
                                                     "description" : null,
                                                     "type" : null,
                                                     "subtype" : null,
                                                     "binding" : "secrets.kafka.bootstrap-servers",
                                                     "required" : false
                                                   } ]
                                                 } ]
                                               }
                                             }""",
                                    result.getResponse().getContentAsString());
                        });
    }

    @Test
    void testDeployFromArchetype() throws Exception {
        mockMvc.perform(put("/api/tenants/my-tenant")).andExpect(status().isOk());
        mockMvc.perform(
                        post("/api/archetypes/my-tenant/simple/applications/app-id")
                                .contentType("application/json")
                                .content(
                                        """
                            {
                               "s1": "value 1",
                               "i1": 50,
                               "r1": 89,
                               "m1": {
                                  "key1": "value 1",
                                  "key2": {
                                     "key2-1": "value 2-1"
                                  }
                               },
                               "m2": "a",
                               "l1": [
                                  "value 1",
                                  "value 2"
                               ],
                               "s2": "value secret 2",
                               "i2": 100,
                               "k2": "value 3"
                            }
                            """))
                .andExpect(status().isOk())
                .andExpect(
                        result -> {
                            ObjectMapper mapper = new ObjectMapper();
                            String body = result.getResponse().getContentAsString();
                            log.info("Deploy result {}", body);
                            Map<String, Object> parse =
                                    mapper.readValue(
                                            body, new TypeReference<Map<String, Object>>() {});
                            Map<String, Object> instance =
                                    (Map<String, Object>) parse.get("instance");
                            Map<String, Object> globals =
                                    (Map<String, Object>) instance.get("globals");
                            assertEquals(50, globals.get("input-value"));
                            assertEquals(89, globals.get("int-value"));
                            assertEquals(List.of("value 1", "value 2"), globals.get("list-value"));
                            assertEquals(
                                    Map.of(
                                            "key1",
                                            "value 1",
                                            "key2",
                                            Map.of("key2-1", "value 2-1")),
                                    globals.get("map-value"));
                            assertEquals(
                                    Map.of(
                                            "key1",
                                            Map.of(
                                                    "key1-1",
                                                    "nested-map-value-1-1",
                                                    "key1-2",
                                                    "nested-map-value-1-2"),
                                            "key2",
                                            Map.of(
                                                    "key2-1",
                                                    "a",
                                                    "key2-2",
                                                    "nested-map-value-2-2")),
                                    globals.get("nested-map"));
                        });

        String codeArchiveReference =
                applicationStore.getSpecs("my-tenant", "app-id").getCodeArchiveReference();
        Secrets secrets = applicationStore.getSecrets("my-tenant", "app-id");
        assertEquals("value secret 2", secrets.secrets().get("open-ai").data().get("foo"));
        assertEquals(100, secrets.secrets().get("open-ai").data().get("foo-int"));
        assertEquals("value 3", secrets.secrets().get("kafka").data().get("bootstrap-servers"));

        byte[] bytes =
                codeStorageService.downloadApplicationCode(
                        "my-tenant", "app-id", codeArchiveReference);
        assertNotNull(bytes);
    }
}
