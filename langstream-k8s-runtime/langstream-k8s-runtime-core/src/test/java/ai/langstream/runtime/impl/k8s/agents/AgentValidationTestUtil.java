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
package ai.langstream.runtime.impl.k8s.agents;

import static org.junit.jupiter.api.Assertions.fail;

import ai.langstream.api.model.Application;
import ai.langstream.api.runtime.ClusterRuntimeRegistry;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.impl.deploy.ApplicationDeployer;
import ai.langstream.impl.parser.ModelBuilder;
import java.util.Map;
import lombok.Cleanup;

public class AgentValidationTestUtil {

    public static void validate(String pipeline, String expectErrMessage) throws Exception {
        validate(pipeline, null, expectErrMessage);
    }

    public static void validate(String pipeline, String configuration, String expectErrMessage)
            throws Exception {
        if (expectErrMessage != null && expectErrMessage.isBlank()) {
            throw new IllegalArgumentException("expectErrMessage cannot be blank");
        }
        if (configuration == null) {
            configuration =
                    """
                    configuration:
                        resources:
                          - type: "datasource"
                            name: "cassandra"
                            configuration:
                              service: "cassandra"
                              contact-points: "xx"
                              loadBalancing-localDc: "xx"
                              port: 999
                          - type: "vector-database"
                            name: "PineconeDatasource"
                            configuration:
                              service: "pinecone"
                              api-key: "xx"
                              index-name: "xx"
                              project-name: "999x"
                              environment: "us-east1"
                    """;
        }
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "module.yaml",
                                        pipeline,
                                        "configuration.yaml",
                                        configuration),
                                """
                                        instance:
                                          streamingCluster:
                                            type: "noop"
                                          computeCluster:
                                            type: "none"
                                        """,
                                null)
                        .getApplication();

        @Cleanup
        ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .pluginsRegistry(new PluginsRegistry())
                        .build();

        try {
            ExecutionPlan implementation =
                    deployer.createImplementation("app", applicationInstance);
            if (expectErrMessage != null) {
                fail("Expected error message instead no errors thrown: " + expectErrMessage);
            }
        } catch (IllegalArgumentException e) {
            if (expectErrMessage != null && e.getMessage().contains(expectErrMessage)) {
                return;
            }
            fail("Expected error message: " + expectErrMessage + " but got: " + e.getMessage());
        }
    }
}
