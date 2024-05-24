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
package ai.langstream.runtime.impl.k8s;

import ai.langstream.api.runtime.DeployContext;
import ai.langstream.api.webservice.application.ApplicationCodeInfo;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class KubernetesClusterRuntimeTest {

    @Test
    public void testTryKeepPreviousCodeArchiveId() throws Exception {
        assertCode(
                Map.of(
                        "code1", codeInfo("python1", "java1"),
                        "code2", codeInfo("python1", "java1")),
                "code1",
                "code2",
                true);

        assertCode(
                Map.of(
                        "code1", codeInfo(null, null),
                        "code2", codeInfo(null, null)),
                "code1",
                "code2",
                true);

        assertCode(
                Map.of(
                        "code1", codeInfo(null, null),
                        "code2", codeInfo("python1", null)),
                "code1",
                "code2",
                false);

        assertCode(
                Map.of(
                        "code1", codeInfo("python1", null),
                        "code2", codeInfo("python1", null)),
                "code1",
                "code2",
                true);

        assertCode(
                Map.of(
                        "code1", codeInfo("python1", null),
                        "code2", codeInfo("python2", null)),
                "code1",
                "code2",
                false);

        assertCode(
                Map.of(
                        "code1", codeInfo(null, null),
                        "code2", codeInfo(null, "java1")),
                "code1",
                "code2",
                false);

        assertCode(
                Map.of(
                        "code1", codeInfo(null, "java1"),
                        "code2", codeInfo(null, "java1")),
                "code1",
                "code2",
                true);

        assertCode(
                Map.of(
                        "code1", codeInfo(null, "java1"),
                        "code2", codeInfo(null, "java2")),
                "code1",
                "code2",
                false);

        assertCode(
                Map.of(
                        "code1", codeInfo(null, "java1"),
                        "code2", codeInfo("python1", "java1")),
                "code1",
                "code2",
                false);

        assertCode(
                Map.of(
                        "code1", codeInfo("python1", "java1"),
                        "code2", codeInfo("python2", "java1")),
                "code1",
                "code2",
                false);
    }

    static ApplicationCodeInfo codeInfo(String python, String java) {
        return new ApplicationCodeInfo(new ApplicationCodeInfo.Digests(python, java));
    }

    private void assertCode(
            Map<String, ApplicationCodeInfo> mapping,
            String newCode,
            String oldCode,
            boolean expectKeep) {
        final DeployContext deployContext =
                new DeployContext.NoOpDeployContext() {
                    @Override
                    public ApplicationCodeInfo getApplicationCodeInfo(
                            String tenant, String applicationId, String codeArchiveId) {
                        final ApplicationCodeInfo mapped = mapping.get(codeArchiveId);
                        if (mapped == null) {
                            throw new RuntimeException();
                        }
                        return mapped;
                    }
                };

        final String result =
                KubernetesClusterRuntime.tryKeepPreviousCodeArchiveId(
                        "tenant", "app1", newCode, oldCode, deployContext);
        if (result.equals(oldCode)) {
            if (!expectKeep) {
                Assertions.fail("Expected to get the new code");
            }
        } else {
            if (expectKeep) {
                Assertions.fail("Expected to keep the old code");
            }
        }
    }
}
