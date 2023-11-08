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

import static org.mockito.ArgumentMatchers.eq;

import ai.langstream.runtime.api.agent.AgentRunnerConstants;
import ai.langstream.runtime.api.agent.RuntimePodConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.nio.file.Files;
import java.util.Map;
import java.util.function.Supplier;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class AgentRunnerStarterTest {

    static ObjectMapper mapper = new ObjectMapper();

    @Test
    public void test() throws Exception {

        String podRuntimeFile =
                Files.createTempFile("langstream", ".json").toFile().getAbsolutePath();
        mapper.writeValue(
                new File(podRuntimeFile), new RuntimePodConfiguration(null, null, null, null));
        String codeDir =
                Files.createTempDirectory("langstream-cli-test").toFile().getAbsolutePath();
        String agentsDir =
                Files.createTempDirectory("langstream-cli-test").toFile().getAbsolutePath();
        String persistentStateDir =
                Files.createTempDirectory("langstream-cli-test").toFile().getAbsolutePath();

        runTest(false, Map.of());
        runTest(false, Map.of(), podRuntimeFile);
        runTest(false, Map.of(), podRuntimeFile, codeDir);
        runTest(
                true,
                Map.of(AgentRunnerConstants.PERSISTENT_VOLUMES_PATH, persistentStateDir),
                podRuntimeFile,
                codeDir,
                agentsDir);
        runTest(
                false,
                Map.of(
                        AgentRunnerConstants.POD_CONFIG_ENV,
                        podRuntimeFile,
                        AgentRunnerConstants.DOWNLOADED_CODE_PATH_ENV,
                        codeDir));
        runTest(
                true,
                Map.of(
                        AgentRunnerConstants.POD_CONFIG_ENV,
                        podRuntimeFile,
                        AgentRunnerConstants.DOWNLOADED_CODE_PATH_ENV,
                        codeDir,
                        AgentRunnerConstants.AGENTS_ENV,
                        agentsDir,
                        AgentRunnerConstants.PERSISTENT_VOLUMES_PATH,
                        persistentStateDir));
    }

    @SneakyThrows
    private void runTest(boolean expectOk, Map<String, String> env, String... args) {
        final AgentRunner agentRunner = Mockito.mock(AgentRunner.class);

        try {
            new TestDeployer(agentRunner, env).start(args);
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
        Mockito.verify(agentRunner)
                .run(
                        Mockito.any(),
                        Mockito.any(),
                        Mockito.any(),
                        Mockito.any(),
                        Mockito.any(),
                        Mockito.any(Supplier.class),
                        Mockito.any(),
                        eq(true),
                        Mockito.any(),
                        Mockito.any());
    }

    static class TestDeployer extends AgentRunnerStarter {
        private final Map<String, String> map;

        public TestDeployer(AgentRunner agentRunner, Map<String, String> map) {
            super(agentRunner);
            this.map = map;
        }

        @Override
        protected String getEnv(String key) {
            return map.get(key);
        }
    }
}
