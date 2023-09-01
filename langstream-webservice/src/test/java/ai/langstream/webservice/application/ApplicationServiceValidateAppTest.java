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

import ai.langstream.api.model.Application;
import ai.langstream.api.runtime.AgentNode;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.impl.k8s.tests.KubeTestServer;
import ai.langstream.impl.parser.ModelBuilder;
import ai.langstream.webservice.config.ApplicationDeployProperties;
import ai.langstream.webservice.config.TenantProperties;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Slf4j
class ApplicationServiceValidateAppTest {

    @RegisterExtension static final KubeTestServer k3s = new KubeTestServer();

    @Test
    void testApplicationId() {
        final Map<String, String> files = filesWithOneAgent(null, null, "s");
        validate(null, files, false);
        validate("", files, false);
        validate("myapp", files, true);
        validate("all-chars09", files, true);
        validate("myapp with spaces", files, false);
        validate("myapp_", files, false);
        validate("9myapp", files, false);
        validate("Umyapp", files, false);
        validate("a".repeat(20), files, true);
        validate("a".repeat(21), files, false);
    }

    @Test
    void testAgentWithFixedId() {
        final String appId = "app";
        validate(appId, filesWithOneAgent(null, null, "agent"), true);
        validate(appId, filesWithOneAgent(null, null, "agent01-"), true);
        validate(appId, filesWithOneAgent(null, null, "a".repeat(37)), true);
        validate(appId, filesWithOneAgent(null, null, "a".repeat(38)), false);
        validate(appId, filesWithOneAgent(null, null, "with spaces"), false);
        validate(appId, filesWithOneAgent(null, null, "Upper"), false);
        validate(appId, filesWithOneAgent(null, null, "0agent"), true);
        validate(appId, filesWithOneAgent(null, null, "0"), true);
    }

    @Test
    void testAgentWithComputedId() {
        final String appId = "app";
        validate(appId, filesWithOneAgent(null, null, null), true);
        validate(appId, filesWithOneAgent("m".repeat(21), null, null), true);
        validate(appId, filesWithOneAgent("m".repeat(24), null, null), false);

        validate(appId, filesWithOneAgent("with spaces", null, null), false);
        validate(appId, filesWithOneAgent("withUpper", null, null), false);
    }

    private Map<String, String> filesWithOneAgent(String module, String pipeline, String agentId) {
        if (pipeline == null) {
            pipeline = "pipeline";
        }
        return Map.of(
                "%s.yaml".formatted(pipeline),
                """
                        module: %s
                        id: %s
                        topics:
                          - name: "input-topic"
                            creation-mode: create-if-not-exists
                          - name: "output-topic"
                            creation-mode: create-if-not-exists
                        pipeline:
                          - id: %s
                            type: "drop"
                            input: "input-topic"
                            output: "output-topic"
                        """
                        .formatted(module, pipeline, agentId));
    }

    @SneakyThrows
    private static void validate(
            String applicationId, Map<String, String> files, boolean expectValid) {
        final ApplicationService service = getApplicationService();
        final Application application =
                ModelBuilder.buildApplicationInstance(
                                files,
                                """
                        instance:
                          streamingCluster:
                            type: "noop"
                          computeCluster:
                            type: "kubernetes"
                        """,
                                null)
                        .getApplication();
        boolean ok = false;
        Throwable exception = null;
        try {
            final ExecutionPlan plan = service.validateExecutionPlan(applicationId, application);
            log.info(
                    "Got agents: {}",
                    plan.getAgents().values().stream().map(AgentNode::getId).toList());
            if (expectValid) {
                ok = true;
            }

        } catch (Exception e) {
            if (!expectValid) {
                ok = true;
                log.info("Got expected exception", e);
            }
            exception = e;
        }
        if (!ok) {
            if (!expectValid) {
                throw new RuntimeException("Expected exception");
            } else {
                throw new RuntimeException(
                        "Expected app to be valid. Instead got: " + exception.getMessage());
            }
        }
    }

    @NotNull
    private static ApplicationService getApplicationService() {
        return new ApplicationService(
                null,
                null,
                new ApplicationDeployProperties(
                        new ApplicationDeployProperties.GatewayProperties(false)),
                new TenantProperties());
    }
}
