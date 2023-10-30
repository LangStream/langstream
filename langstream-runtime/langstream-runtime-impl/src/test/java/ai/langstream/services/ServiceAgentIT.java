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
package ai.langstream.services;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.langstream.mockagents.MockProcessorAgentsCodeProvider;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
class ServiceAgentIT extends AbstractStreamingLessApplicationRunner {

    @Test
    public void testService() throws Exception {

        MockProcessorAgentsCodeProvider.MockService.resetCounters();

        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};

        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                                pipeline:
                                  - name: "Service"
                                    type: "mock-service"
                                    id: step1
                                """);

        // query the database with re-rank
        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            executeAgentRunners(applicationRuntime);

            assertEquals(1, MockProcessorAgentsCodeProvider.MockService.startCounters.get());
            assertEquals(1, MockProcessorAgentsCodeProvider.MockService.joinCounter.get());
            assertEquals(1, MockProcessorAgentsCodeProvider.MockService.closeCounter.get());
        }
    }
}
