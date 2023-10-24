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

import static ai.langstream.cli.utils.ApplicationPackager.buildZip;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.mock.web.MockPart;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockMultipartHttpServletRequestBuilder;

public class AppTestHelper {

    private static File createTempFile(String content) {
        try {
            Path tempFile =
                    Files.createTempFile(
                            Files.createTempDirectory(""), "langstream-cli-test", ".yaml");
            Files.writeString(tempFile, content);
            return tempFile.toFile();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static MockMultipartFile getMultipartFile(String application) throws Exception {
        final Path zip =
                buildZip(
                        application == null ? null : createTempFile(application),
                        System.out::println);
        return new MockMultipartFile(
                "app",
                "content.zip",
                MediaType.APPLICATION_OCTET_STREAM_VALUE,
                Files.newInputStream(zip));
    }

    public static ResultActions deployApp(
            MockMvc mockMvc,
            String tenant,
            String applicationId,
            String appFileContent,
            String instanceContent,
            String secretsContent)
            throws Exception {
        return updateApp(
                mockMvc,
                false,
                tenant,
                applicationId,
                appFileContent,
                instanceContent,
                secretsContent,
                true);
    }

    public static ResultActions deployApp(
            MockMvc mockMvc,
            String tenant,
            String applicationId,
            String appFileContent,
            String instanceContent,
            String secretsContent,
            boolean checkOk)
            throws Exception {
        return updateApp(
                mockMvc,
                false,
                tenant,
                applicationId,
                appFileContent,
                instanceContent,
                secretsContent,
                checkOk);
    }

    public static ResultActions updateApp(
            MockMvc mockMvc,
            String tenant,
            String applicationId,
            String appFileContent,
            String instanceContent,
            String secretsContent)
            throws Exception {
        return updateApp(
                mockMvc,
                true,
                tenant,
                applicationId,
                appFileContent,
                instanceContent,
                secretsContent,
                true);
    }

    public static ResultActions updateApp(
            MockMvc mockMvc,
            boolean patch,
            String tenant,
            String applicationId,
            String appFileContent,
            String instanceContent,
            String secretsContent,
            boolean checkOk)
            throws Exception {
        return updateApp(
                mockMvc,
                patch,
                tenant,
                applicationId,
                appFileContent,
                instanceContent,
                secretsContent,
                checkOk,
                null);
    }

    public static ResultActions updateApp(
            MockMvc mockMvc,
            boolean patch,
            String tenant,
            String applicationId,
            String appFileContent,
            String instanceContent,
            String secretsContent,
            boolean checkOk,
            Map<String, String> queryString)
            throws Exception {
        final String queryStringStr =
                queryString == null
                        ? ""
                        : queryString.entrySet().stream()
                                .map(e -> e.getKey() + "=" + e.getValue())
                                .reduce((a, b) -> a + "&" + b)
                                .orElse("");
        final MockMultipartHttpServletRequestBuilder multipart =
                multipart(
                        patch ? HttpMethod.PATCH : HttpMethod.POST,
                        "/api/applications/%s/%s?%s"
                                .formatted(tenant, applicationId, queryStringStr));
        if (appFileContent != null) {
            multipart.file(getMultipartFile(appFileContent));
        }
        if (instanceContent != null) {
            multipart.part(
                    new MockPart("instance", instanceContent.getBytes(StandardCharsets.UTF_8)));
        }
        if (secretsContent != null) {
            multipart.part(
                    new MockPart("secrets", secretsContent.getBytes(StandardCharsets.UTF_8)));
        }
        final ResultActions perform = mockMvc.perform(multipart);
        if (checkOk) {
            return perform.andExpect(status().isOk());
        }
        return perform;
    }
}
