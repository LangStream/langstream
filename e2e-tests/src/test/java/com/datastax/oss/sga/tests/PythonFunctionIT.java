/**
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
package com.datastax.oss.sga.tests;

import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class PythonFunctionIT extends BaseEndToEndTest {

    @Test
    public void test() throws Exception {
        final String tenant = "ten-" + System.currentTimeMillis();
        executeCliCommand("configure", "webServiceUrl", controlPlaneBaseUrl);
        executeCliCommand("configure", "apiGatewayUrl", apiGatewayBaseUrl);
        executeCliCommand("tenants", "put", tenant);
        executeCliCommand("configure", "tenant", tenant);
        String testAppsBaseDir = "src/test/resources/apps";
        String testInstanceBaseDir = "src/test/resources/instances";
        String testSecretBaseDir = "src/test/resources/secrets";
        final String applicationId = "my-test-app";
        executeCliCommand("apps", "deploy", applicationId,
                "-app", Paths.get(testAppsBaseDir, "python-function").toFile().getAbsolutePath(),
                "-i", Paths.get(testInstanceBaseDir, "kafka-kubernetes.yaml").toFile().getAbsolutePath(),
                "-s", Paths.get(testSecretBaseDir, "secret1.yaml").toFile().getAbsolutePath());

        client.apps()
                        .statefulSets()
                .inNamespace(TENANT_NAMESPACE_PREFIX + tenant)
                .withName(applicationId + "-module-1-pipeline-1-python-function-1")
                .waitUntilReady(3, TimeUnit.MINUTES);


        executeCliCommand("gateway", "produce", applicationId, "produce-input", "-v", "my-value");
        executeCliCommandUntilOutput(new Predicate<String>() {
            @Override
            public boolean test(String s) {
                if (s.startsWith("{\"record\":{\"key\":null,\"value\":\"my-value!!super secret value\",\"headers\":{}}")) {
                    return true;
                }
                System.out.println("Got line: " + s);
                return false;
            }
        }, "gateway", "consume", applicationId, "consume-output", "--position", "earliest");
    }

}

