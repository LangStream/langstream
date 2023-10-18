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
package ai.langstream.impl.resources;

import static ai.langstream.impl.resources.ResourceNodeProviderTest.Outcome.NON_VALID;
import static ai.langstream.impl.resources.ResourceNodeProviderTest.Outcome.VALID;
import static org.junit.jupiter.api.Assertions.*;

import ai.langstream.api.model.Resource;
import ai.langstream.api.runtime.ResourceNodeProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ResourceNodeProviderTest {

    enum Outcome {
        VALID,
        NON_VALID
    }

    private void test(
            Outcome outcome,
            String type,
            Map<String, Object> configuration,
            ResourceNodeProvider providersResourceProvider) {
        Resource resource = new Resource("the-id", "then-name", type, configuration);
        switch (outcome) {
            case VALID -> {
                assertDoesNotThrow(
                        () -> providersResourceProvider.createImplementation(resource, null));
            }
            case NON_VALID -> {
                assertThrows(
                        IllegalArgumentException.class,
                        () -> providersResourceProvider.createImplementation(resource, null));
            }
        }
    }

    private static List<Arguments> configurationsAIProviders() {
        return Arrays.asList(
                Arguments.of(NON_VALID, "open-ai-configuration", Map.of()),
                Arguments.of(NON_VALID, "vertex-configuration", Map.of()),
                Arguments.of(VALID, "hugging-face-configuration", Map.of()),
                Arguments.of(
                        VALID,
                        "open-ai-configuration",
                        Map.of("access-key", "the-api-key", "provider", "openai")),
                Arguments.of(
                        NON_VALID,
                        "open-ai-configuration",
                        Map.of("access-key", "the-api-key", "provider", "azure")),
                Arguments.of(
                        VALID,
                        "open-ai-configuration",
                        Map.of(
                                "access-key",
                                "the-api-key",
                                "provider",
                                "azure",
                                "url",
                                "http://some-url")),
                Arguments.of(VALID, "hugging-face-configuration", Map.of("provider", "api")),
                Arguments.of(VALID, "hugging-face-configuration", Map.of("provider", "local")),
                Arguments.of(
                        NON_VALID,
                        "hugging-face-configuration",
                        Map.of("provider", "bad-provider")),
                Arguments.of(
                        VALID,
                        "vertex-configuration",
                        Map.of(
                                "url",
                                "http://some-url",
                                "region",
                                "us-east-1",
                                "project",
                                "some-project",
                                "token",
                                "some-token")),
                Arguments.of(
                        VALID,
                        "vertex-configuration",
                        Map.of(
                                "url",
                                "http://some-url",
                                "region",
                                "us-east-1",
                                "project",
                                "some-project",
                                "serviceAccountJson",
                                "{\"some\": \"json\"}")),
                Arguments.of(
                        NON_VALID,
                        "vertex-configuration",
                        Map.of(
                                "url",
                                "http://some-url",
                                "region",
                                "us-east-1",
                                "project",
                                "some-project",
                                "serviceAccountJson",
                                "not-a-valid-json")),
                Arguments.of(
                        NON_VALID,
                        "vertex-configuration",
                        Map.of(
                                "url",
                                "http://some-url",
                                "region",
                                "us-east-1",
                                "project",
                                "some-project",
                                "token",
                                "some-token",
                                "serviceAccountJson",
                                "{\"some\": \"json\"}")),
                Arguments.of(
                        NON_VALID,
                        "vertex-configuration",
                        Map.of(
                                "url",
                                "http://some-url",
                                "region",
                                "us-east-1",
                                "project",
                                "some-project")),
                Arguments.of(
                        NON_VALID,
                        "vertex-configuration",
                        Map.of(
                                "url",
                                "http://some-url",
                                "region",
                                "us-east-1",
                                "token",
                                "some-token")),
                Arguments.of(
                        NON_VALID,
                        "vertex-configuration",
                        Map.of(
                                "url",
                                "http://some-url",
                                "project",
                                "some-project",
                                "token",
                                "some-token")),
                Arguments.of(
                        NON_VALID,
                        "vertex-configuration",
                        Map.of(
                                "region",
                                "us-east-1",
                                "project",
                                "some-project",
                                "token",
                                "some-token")),
                Arguments.of(NON_VALID, "bedrock-configuration", Map.of()),
                Arguments.of(
                        VALID,
                        "bedrock-configuration",
                        Map.of(
                                "region", "us-west-2",
                                "access-key", "xx",
                                "secret-key", "yy")));
    }

    @ParameterizedTest
    @MethodSource("configurationsAIProviders")
    void testAIConfigurationResource(
            Outcome outcome, String type, Map<String, Object> configuration) {
        test(outcome, type, configuration, new AIProvidersResourceProvider());
    }

    private static List<Arguments> configurationsDataSourceProviders() {
        return Arrays.asList(
                Arguments.of(NON_VALID, "cassandra", Map.of()),
                Arguments.of(NON_VALID, "jdbc", Map.of()),
                Arguments.of(NON_VALID, "astra", Map.of()),
                Arguments.of(
                        VALID,
                        "cassandra",
                        Map.of(
                                "contact-points",
                                "localhost",
                                "loadBalancing-localDc",
                                "dc1",
                                "username",
                                "user",
                                "password",
                                "pwd")),
                Arguments.of(
                        VALID,
                        "cassandra",
                        Map.of(
                                "contact-points",
                                "localhost",
                                "loadBalancing-localDc",
                                "dc1",
                                "username",
                                "user",
                                "password",
                                "pwd")),
                Arguments.of(
                        NON_VALID,
                        "cassandra",
                        Map.of(
                                "contact-points",
                                "locahost",
                                "username",
                                "user",
                                "password",
                                "pwd")),
                Arguments.of(
                        NON_VALID,
                        "cassandra",
                        Map.of(
                                "loadBalancing-localDc",
                                "dc1",
                                "username",
                                "user",
                                "password",
                                "pwd")),
                Arguments.of(
                        NON_VALID,
                        "cassandra",
                        Map.of(
                                "secureBundle",
                                "base64:" + Base64.getEncoder().encodeToString("foo".getBytes()),
                                "username",
                                "user",
                                "password",
                                "pwd")),
                Arguments.of(
                        VALID,
                        "astra",
                        Map.of(
                                "secureBundle",
                                "base64:" + Base64.getEncoder().encodeToString("foo".getBytes()),
                                "clientId",
                                "user",
                                "secret",
                                "pwd")),
                Arguments.of(
                        NON_VALID,
                        "astra",
                        Map.of(
                                "secureBundle",
                                "base64:" + Base64.getEncoder().encodeToString("foo".getBytes()),
                                "username",
                                "user",
                                "password",
                                "pwd")),
                Arguments.of(
                        NON_VALID,
                        "astra",
                        Map.of(
                                "secureBundle",
                                Base64.getEncoder().encodeToString("foo".getBytes()),
                                "username",
                                "user",
                                "password",
                                "pwd")),
                Arguments.of(
                        NON_VALID,
                        "astra",
                        Map.of(
                                "secureBundle",
                                "not-base-64",
                                "username",
                                "user",
                                "password",
                                "pwd")),
                Arguments.of(
                        VALID,
                        "astra",
                        Map.of(
                                "token",
                                "the-token",
                                "database",
                                "the-database",
                                "clientId",
                                "user",
                                "secret",
                                "pwd")),
                Arguments.of(
                        NON_VALID,
                        "astra",
                        Map.of("token", "the-token", "clientId", "user", "secret", "pwd")),
                Arguments.of(
                        NON_VALID,
                        "astra",
                        Map.of("database", "the-database", "clientId", "user", "secret", "pwd")),
                Arguments.of(NON_VALID, "astra", Map.of("clientId", "user", "secret", "pwd")),
                Arguments.of(NON_VALID, "jdbc", Map.of("driverClass", "some.java.Class")),
                Arguments.of(
                        NON_VALID,
                        "jdbc",
                        Map.of(
                                "driverClass",
                                "some.java.Class",
                                "usermame",
                                "user",
                                "password",
                                "pass")));
    }

    private static List<Arguments> configurationsVectorDatabaseProviders() {
        List<Arguments> all = new ArrayList<>();
        all.addAll(configurationsDataSourceProviders());
        all.addAll(
                (Arrays.asList(
                        Arguments.of(NON_VALID, "pinecone", Map.of()),
                        Arguments.of(
                                VALID,
                                "pinecone",
                                Map.of(
                                        "api-key",
                                        "xxx",
                                        "environment",
                                        "xxx",
                                        "project-name",
                                        "xxx",
                                        "index-name",
                                        "xxx")),
                        Arguments.of(
                                VALID,
                                "pinecone",
                                Map.of(
                                        "api-key",
                                        "xxx",
                                        "environment",
                                        "xxx",
                                        "project-name",
                                        "xxx",
                                        "index-name",
                                        "xxx",
                                        "server-side-timeout-sec",
                                        10000)),
                        Arguments.of(
                                VALID,
                                "pinecone",
                                Map.of(
                                        "api-key",
                                        "xxx",
                                        "environment",
                                        "xxx",
                                        "project-name",
                                        "xxx",
                                        "index-name",
                                        "xxx",
                                        "server-side-timeout-sec",
                                        "10000")),
                        Arguments.of(
                                NON_VALID,
                                "pinecone",
                                Map.of(
                                        "api-key",
                                        "xxx",
                                        "environment",
                                        "xxx",
                                        "project-name",
                                        "xxx",
                                        "index-name",
                                        "xxx",
                                        "server-side-timeout-sec",
                                        -12)),
                        Arguments.of(
                                NON_VALID,
                                "pinecone",
                                Map.of(
                                        "api-key",
                                        "xxx",
                                        "environment",
                                        "xxx",
                                        "project-name",
                                        "xxx")),
                        Arguments.of(
                                NON_VALID,
                                "pinecone",
                                Map.of(
                                        "api-key",
                                        "xxx",
                                        "environment",
                                        "xxx",
                                        "index-name",
                                        "xxx")),
                        Arguments.of(
                                NON_VALID,
                                "pinecone",
                                Map.of(
                                        "api-key",
                                        "xxx",
                                        "project-name",
                                        "xxx",
                                        "index-name",
                                        "xxx")),
                        Arguments.of(
                                NON_VALID,
                                "pinecone",
                                Map.of(
                                        "environment",
                                        "xxx",
                                        "project-name",
                                        "xxx",
                                        "index-name",
                                        "xxx")))));
        return all;
    }

    @ParameterizedTest
    @MethodSource("configurationsDataSourceProviders")
    void testDataSourceNodeResourceProvider(
            Outcome outcome, String service, Map<String, Object> configuration) {
        Map<String, Object> resourceConfiguration = new HashMap<>();
        resourceConfiguration.putAll(configuration);
        resourceConfiguration.put("service", service);
        test(outcome, "datasource", resourceConfiguration, new DataSourceResourceProvider());
    }

    @ParameterizedTest
    @MethodSource("configurationsVectorDatabaseProviders")
    void testVectorDatabaseNodeResourceProvider(
            Outcome outcome, String service, Map<String, Object> configuration) {
        Map<String, Object> resourceConfiguration = new HashMap<>();
        resourceConfiguration.putAll(configuration);
        resourceConfiguration.put("service", service);
        test(outcome, "datasource", resourceConfiguration, new VectorDatabaseResourceProvider());
    }
}
