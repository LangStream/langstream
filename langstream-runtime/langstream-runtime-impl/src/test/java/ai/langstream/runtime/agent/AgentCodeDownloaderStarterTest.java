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
import ai.langstream.runtime.api.agent.AgentRunnerConstants;
import ai.langstream.runtime.api.agent.DownloadAgentCodeConfiguration;
import ai.langstream.runtime.api.agent.RuntimePodConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.nio.file.Files;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class AgentCodeDownloaderStarterTest {


    static ObjectMapper mapper = new ObjectMapper();

    @Test
    public void test() throws Exception {

        String clusterConfigFile = Files.createTempFile("langstream", ".json").toFile().getAbsolutePath();
        mapper.writeValue(new File(clusterConfigFile), new ClusterConfiguration("http://localhost:8080"));
        String downloaderConfigFile = Files.createTempFile("langstream", ".json").toFile().getAbsolutePath();
        mapper.writeValue(new File(downloaderConfigFile), new DownloadAgentCodeConfiguration(null, null, null, null));


        runTest(false, Map.of());
        runTest(false, Map.of(AgentCodeDownloaderConstants.CLUSTER_CONFIG_ENV, clusterConfigFile));
        runTest(false, Map.of(AgentCodeDownloaderConstants.DOWNLOAD_CONFIG_ENV, downloaderConfigFile));
        runTest(true, Map.of(AgentCodeDownloaderConstants.CLUSTER_CONFIG_ENV, clusterConfigFile,
                AgentCodeDownloaderConstants.DOWNLOAD_CONFIG_ENV, downloaderConfigFile));
    }

    @SneakyThrows
    private void runTest(boolean expectOk, Map<String, String> env, String... args) {
        final AgentCodeDownloader codeDownloader = Mockito.mock(AgentCodeDownloader.class);

        try {
            new TestDeployer(env).run(codeDownloader, args);
        } catch (Exception e) {
            if (expectOk) {
                throw new RuntimeException(e);
            } else {
                return;
            }
        }
        if (!expectOk) {
            throw new RuntimeException("Expected exception");
        }
        Mockito.verify(codeDownloader).downloadCustomCode(Mockito.any(), Mockito.anyString(), Mockito.any());

    }

    static class TestDeployer extends AgentCodeDownloaderStarter {
        private final Map<String, String> map;

        public TestDeployer(Map<String, String> map) {
            this.map = map;
        }

        @Override
        String getEnv(String key) {
            return map.get(key);
        }
    }


}