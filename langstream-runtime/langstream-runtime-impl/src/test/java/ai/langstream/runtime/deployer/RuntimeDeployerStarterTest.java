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
package ai.langstream.runtime.deployer;

import ai.langstream.api.model.Secrets;
import ai.langstream.runtime.api.agent.AgentRunnerConstants;
import ai.langstream.runtime.api.deployer.RuntimeDeployerConfiguration;
import ai.langstream.runtime.api.deployer.RuntimeDeployerConstants;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@Slf4j
class RuntimeDeployerStarterTest {

    public static final Path agentsDirectory;

    static {
        agentsDirectory = Path.of(System.getProperty("user.dir"), "target", "agents");
        log.info("Agents directory is {}", agentsDirectory);
    }

    static ObjectMapper mapper = new ObjectMapper();

    @Test
    public void test() throws Exception {

        String clusterRuntimeFile =
                Files.createTempFile("langstream", ".json").toFile().getAbsolutePath();
        mapper.writeValue(new File(clusterRuntimeFile), Map.of("config", "config"));

        String appConfigFile =
                Files.createTempFile("langstream-cli-test", ".json").toFile().getAbsolutePath();
        mapper.writeValue(new File(appConfigFile), new RuntimeDeployerConfiguration());

        String secretsConfigFile =
                Files.createTempFile("langstream-cli-test", ".json").toFile().getAbsolutePath();
        mapper.writeValue(new File(secretsConfigFile), new Secrets(null));

        runTest(false, Map.of());
        runTest(false, Map.of(), "");
        runTest(false, Map.of(), "unknown");

        runTest(false, Map.of(), "deploy");
        runTest(false, Map.of(), "deploy", clusterRuntimeFile);
        runTest(true, Map.of(), "deploy", clusterRuntimeFile, appConfigFile);
        runTest(true, Map.of(), "deploy", clusterRuntimeFile, appConfigFile, secretsConfigFile);
        runTest(
                true,
                Map.of(
                        RuntimeDeployerConstants.APP_CONFIG_ENV,
                        appConfigFile,
                        RuntimeDeployerConstants.CLUSTER_RUNTIME_CONFIG_ENV,
                        clusterRuntimeFile),
                "deploy");
        runTest(
                true,
                Map.of(
                        RuntimeDeployerConstants.APP_CONFIG_ENV,
                        appConfigFile,
                        RuntimeDeployerConstants.CLUSTER_RUNTIME_CONFIG_ENV,
                        clusterRuntimeFile,
                        RuntimeDeployerConstants.APP_SECRETS_ENV,
                        secretsConfigFile),
                "deploy");
    }

    @SneakyThrows
    private void runTest(boolean expectOk, Map<String, String> env, String... args) {
        Map<String, String> envWithAgents = new HashMap<>(env);
        envWithAgents.put(AgentRunnerConstants.AGENTS_ENV, agentsDirectory.toString());
        final RuntimeDeployer runtimeDeployer = Mockito.mock(RuntimeDeployer.class);

        try {
            new TestDeployer(envWithAgents, runtimeDeployer).start(args);
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
        Mockito.verify(runtimeDeployer)
                .deploy(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    }

    static class TestDeployer extends RuntimeDeployerStarter {
        private final Map<String, String> map;

        public TestDeployer(Map<String, String> map, RuntimeDeployer runtimeDeployer) {
            super(runtimeDeployer);
            this.map = map;
        }

        @Override
        protected String getEnv(String key) {
            return map.get(key);
        }
    }
}
