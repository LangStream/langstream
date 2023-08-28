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
package ai.langstream.model.parser;

import ai.langstream.api.model.Application;
import ai.langstream.impl.common.ApplicationPlaceholderResolver;
import ai.langstream.impl.parser.ModelBuilder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
class ModelBuilderTest {

    @Test
    void testParseApplication1() throws Exception {
        Path path = Paths.get("src/test/resources/application1");
        final ModelBuilder.ApplicationWithPackageInfo applicationWithPackageInfo =
                ModelBuilder.buildApplicationInstance(List.of(path), """
                                        instance:
                                          globals:
                                            tableName: "vsearch.products"
                                          streamingCluster:
                                            type: "pulsar"
                                            configuration:
                                              admin:
                                                serviceUrl: "locahost:9999"
                                              defaultTenant: "public"
                                              defaultNamespace: "default"
                                          computeCluster:
                                            type: "pulsar"
                        """, """
                                                
                        secrets:
                          - name: "Database credentials"
                            id: "astra-credentials"
                            data:
                              username: "my-db-username"
                              password: "my-db-password"
                              secureBundle: "base64:AAAAAAAAAAAAAAA"
                          - name: "OpenAI Azure credentials"
                            id: "openai-credentials"
                            data:
                               accessKey: "xxxxxxxxx"
                          - name: "Hugging Face credentials"
                            id: "hf-credentials"
                            data:
                              accessKey: "xxxxxxxxx"
                        """);
        log.info("Parsed {}", applicationWithPackageInfo.getApplication());
        Application resolved = ApplicationPlaceholderResolver.resolvePlaceholders(applicationWithPackageInfo.getApplication());
        log.info("Resolved {}", resolved);
    }
}
