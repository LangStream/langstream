package ai.langstream.cli.commands.applications;

import static org.junit.jupiter.api.Assertions.*;
import ai.langstream.admin.client.AdminClient;
import ai.langstream.admin.client.AdminClientConfiguration;
import ai.langstream.admin.client.AdminClientLogger;
import ai.langstream.admin.client.HttpRequestFailedException;
import ai.langstream.admin.client.http.HttpClientFacade;
import ai.langstream.admin.client.http.HttpClientProperties;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@WireMockTest
class AbstractDeployApplicationCmdTest {

    protected WireMock wireMock;
    protected String wireMockBaseUrl;

    @BeforeEach
    public void beforeEach(WireMockRuntimeInfo wmRuntimeInfo)
            throws Exception {
        wireMock = wmRuntimeInfo.getWireMock();
        wireMockBaseUrl = wmRuntimeInfo.getHttpBaseUrl();
    }

    @Test
    void testRemoteFile() throws Exception {
        wireMock.register(
                WireMock.get("/my-remote-dir/my-remote-file")
                        .willReturn(WireMock.ok("content!")));


        final HttpClientFacade client = new HttpClientFacade(new AdminClientLogger() {
            @Override
            public void log(Object message) {
                System.out.println(message);
            }

            @Override
            public void error(Object message) {
                System.err.println(message);
            }

            @Override
            public boolean isDebugEnabled() {
                return false;
            }

            @Override
            public void debug(Object message) {
            }
        }, new HttpClientProperties());
        final File file = AbstractDeployApplicationCmd.downloadHttpsFile(
                wireMockBaseUrl + "/my-remote-dir/my-remote-file", client, log -> System.out.println(log));
        assertEquals("content!", Files.readString(file.toPath()));

        try {
            AbstractDeployApplicationCmd.downloadHttpsFile(
                    wireMockBaseUrl + "/unknown", client, log -> System.out.println(log));
            fail();
        } catch (HttpRequestFailedException ex) {
            assertEquals(404, ex.getResponse().statusCode());
        }
    }

}