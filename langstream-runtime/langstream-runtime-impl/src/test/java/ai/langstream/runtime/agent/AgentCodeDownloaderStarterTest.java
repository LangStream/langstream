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
package ai.langstream.runtime.agent;

import static org.junit.jupiter.api.Assertions.*;

import ai.langstream.runtime.api.ClusterConfiguration;
import ai.langstream.runtime.api.agent.AgentCodeDownloaderConstants;
import ai.langstream.runtime.api.agent.DownloadAgentCodeConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.nio.file.Files;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class AgentCodeDownloaderStarterTest {

    static ObjectMapper mapper = new ObjectMapper();

    @Test
    public void test() throws Exception {

        String clusterConfigFile =
                Files.createTempFile("langstream", ".json").toFile().getAbsolutePath();
        mapper.writeValue(
                new File(clusterConfigFile), new ClusterConfiguration("http://localhost:8080"));
        String downloaderConfigFile =
                Files.createTempFile("langstream", ".json").toFile().getAbsolutePath();
        mapper.writeValue(
                new File(downloaderConfigFile),
                new DownloadAgentCodeConfiguration(null, null, null, null));
        String tokenConfigFile =
                Files.createTempFile("langstream", ".json").toFile().getAbsolutePath();
        mapper.writeValue(new File(tokenConfigFile), "mytoken");

        runTest(false, Map.of(), false);
        runTest(
                false,
                Map.of(AgentCodeDownloaderConstants.CLUSTER_CONFIG_ENV, clusterConfigFile),
                false);
        runTest(
                false,
                Map.of(AgentCodeDownloaderConstants.DOWNLOAD_CONFIG_ENV, downloaderConfigFile),
                false);
        runTest(
                true,
                Map.of(
                        AgentCodeDownloaderConstants.CLUSTER_CONFIG_ENV,
                        clusterConfigFile,
                        AgentCodeDownloaderConstants.DOWNLOAD_CONFIG_ENV,
                        downloaderConfigFile),
                false);
        runTest(
                true,
                Map.of(
                        AgentCodeDownloaderConstants.CLUSTER_CONFIG_ENV,
                        clusterConfigFile,
                        AgentCodeDownloaderConstants.DOWNLOAD_CONFIG_ENV,
                        downloaderConfigFile,
                        AgentCodeDownloaderConstants.TOKEN_ENV,
                        tokenConfigFile),
                true);
    }

    @SneakyThrows
    private void runTest(
            boolean expectOk, Map<String, String> env, boolean expectToken, String... args) {
        final AgentCodeDownloader codeDownloader = Mockito.mock(AgentCodeDownloader.class);

        try {
            new TestDeployer(codeDownloader, env).start(args);
        } catch (Exception e) {
            if (expectOk) {
                throw new RuntimeException(e);
            } else {
                if (e.getMessage().contains("File") && e.getMessage().contains("does not exist")) {
                    return;
                }
                e.printStackTrace();
                Assertions.fail("expected file not found exception but got " + e.getMessage());
            }
        }
        if (!expectOk) {
            throw new RuntimeException("Expected exception");
        }
        if (expectToken) {
            Mockito.verify(codeDownloader)
                    .downloadCustomCode(Mockito.any(), Mockito.anyString(), Mockito.any());
        } else {
            Mockito.verify(codeDownloader)
                    .downloadCustomCode(Mockito.any(), Mockito.isNull(), Mockito.any());
        }
    }

    static class TestDeployer extends AgentCodeDownloaderStarter {
        private final Map<String, String> map;

        public TestDeployer(AgentCodeDownloader agentCodeDownloader, Map<String, String> map) {
            super(agentCodeDownloader);
            this.map = map;
        }

        @Override
        protected String getEnv(String key) {
            return map.get(key);
        }
    }
}
