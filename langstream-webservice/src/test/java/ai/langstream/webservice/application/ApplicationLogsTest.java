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

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import ai.langstream.impl.k8s.tests.KubeTestServer;
import ai.langstream.webservice.WebAppTestConfig;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import java.net.HttpURLConnection;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest(properties = {"application.tenants.default-tenant.create=false"})
@AutoConfigureMockMvc
@Slf4j
@Import(WebAppTestConfig.class)
@DirtiesContext
class ApplicationLogsTest {

    @Autowired MockMvc mockMvc;

    @RegisterExtension static final KubeTestServer k3s = new KubeTestServer();

    @Test
    void test() throws Exception {
        k3s.expectTenantCreated("default");
        mockMvc.perform(put("/api/tenants/default")).andExpect(status().isOk());

        k3s.spyApplicationCustomResources("langstream-default", "test");
        AppTestHelper.deployApp(
                mockMvc,
                "default",
                "test",
                """
                        id: app1
                        name: test
                        topics: []
                        pipeline: []
                        """,
                """
                        instance:
                          streamingCluster:
                            type: pulsar
                          computeCluster:
                            type: none
                        """,
                null);

        final Pod pod1 = Mockito.mock(Pod.class);

        when(pod1.getMetadata()).thenReturn(new ObjectMetaBuilder().withName("pod1").build());
        final Pod pod2 = Mockito.mock(Pod.class);
        when(pod2.getMetadata()).thenReturn(new ObjectMetaBuilder().withName("pod2").build());
        k3s.getServer()
                .expect()
                .get()
                .withPath(
                        "/api/v1/namespaces/langstream-default/pods?labelSelector=app%3Dlangstream-runtime"
                                + "%2Clangstream-application%3Dtest")
                .andReply(
                        HttpURLConnection.HTTP_OK,
                        recordedRequest -> {
                            final PodList res = new PodList();
                            res.setItems(List.of(pod1, pod2));
                            return res;
                        })
                .always();

        mockMvc.perform(get("/api/applications/default/test/logs")).andExpect(status().isOk());
    }
}
