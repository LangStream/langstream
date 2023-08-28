package ai.langstream.webservice.application;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import ai.langstream.cli.commands.applications.AbstractDeployApplicationCmd;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
            Path tempFile = Files.createTempFile(Files.createTempDirectory(""), "langstream-cli-test", ".yaml");
            Files.writeString(tempFile, content);
            return tempFile.toFile();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }



    private static MockMultipartFile getMultipartFile(String application) throws Exception {
        final Path zip = AbstractDeployApplicationCmd.buildZip(
                application == null ? null : createTempFile(application), System.out::println);
        return new MockMultipartFile(
                "app", "content.zip", MediaType.APPLICATION_OCTET_STREAM_VALUE,
                Files.newInputStream(zip));
    }

    public static ResultActions deployApp(MockMvc mockMvc,
                                          String tenant,
                                          String applicationId,
                                          String appFileContent,
                                          String instanceContent,
                                          String secretsContent) throws Exception {
        return updateApp(mockMvc, false, tenant, applicationId, appFileContent, instanceContent, secretsContent);

    }
    public static ResultActions updateApp(MockMvc mockMvc,
                                          String tenant,
                                          String applicationId,
                                          String appFileContent,
                                          String instanceContent,
                                          String secretsContent) throws Exception {
        return updateApp(mockMvc, true, tenant, applicationId, appFileContent, instanceContent, secretsContent);
    }



    public static ResultActions updateApp(MockMvc mockMvc,
                                          boolean patch,
                                          String tenant,
                                          String applicationId,
                                          String appFileContent,
                                          String instanceContent,
                                          String secretsContent) throws Exception {
        final MockMultipartHttpServletRequestBuilder multipart =
                multipart(patch ? HttpMethod.PATCH : HttpMethod.POST, "/api/applications/%s/%s".formatted(tenant, applicationId));
        if (appFileContent != null) {
            multipart.file(getMultipartFile(appFileContent));
        }
        if (instanceContent != null) {
            multipart.part(new MockPart("instance", instanceContent.getBytes(StandardCharsets.UTF_8)));
        }
        if (secretsContent != null) {
            multipart.part(new MockPart("secrets", secretsContent.getBytes(StandardCharsets.UTF_8)));
        }
        return mockMvc
                .perform(multipart)
                .andExpect(status().isOk());
    }
}
