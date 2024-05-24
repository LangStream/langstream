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
import ai.langstream.api.model.ApplicationSpecs;
import ai.langstream.api.model.Secrets;
import ai.langstream.api.model.StoredApplication;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.storage.ApplicationStore;
import ai.langstream.api.webservice.tenant.TenantConfiguration;
import ai.langstream.api.webservice.tenant.UpdateTenantRequest;
import ai.langstream.impl.k8s.tests.KubeTestServer;
import ai.langstream.impl.parser.ModelBuilder;
import ai.langstream.webservice.WebAppTestConfig;
import ai.langstream.webservice.common.GlobalMetadataService;
import ai.langstream.webservice.config.ApplicationDeployProperties;
import ai.langstream.webservice.config.StorageProperties;
import ai.langstream.webservice.config.TenantProperties;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Slf4j
class ApplicationServiceResourceLimitTest {

    @RegisterExtension static final KubeTestServer k3s = new KubeTestServer();
    protected static final StorageProperties STORAGE_PROPERTIES =
            WebAppTestConfig.buildStorageProperties();

    @Test
    void test() {
        new GlobalMetadataService(STORAGE_PROPERTIES, new TestApplicationStore(Map.of()), null)
                .putTenant("tenant", TenantConfiguration.builder().build());

        putTenantLimit(0);
        validate("app1", filesWithTwoAgents(100, 100), Map.of(), 0, true);
        validate("app1", filesWithTwoAgents(1, 2), Map.of(), 1, false);
        validate("app1", filesWithTwoAgents(1, 2), Map.of(), 2, true);
        validate("app1", filesWithTwoAgents(1, 2), Map.of(), 3, true);

        validate("app1", filesWithTwoAgents(2, 1), Map.of(), 1, false);
        validate("app1", filesWithTwoAgents(2, 1), Map.of(), 2, true);
        validate("app1", filesWithTwoAgents(2, 1), Map.of(), 3, true);

        validate("app1", filesWithTwoAgents(1, 2), Map.of("app1", 1), 1, false);
        validate("app1", filesWithTwoAgents(1, 2), Map.of("app1", 2), 2, true);

        validate("app1", filesWithTwoAgents(1, 2), Map.of("app1", 2, "app2", 2), 4, true);
        validate("app3", filesWithTwoAgents(1, 2), Map.of("app1", 2, "app2", 2), 4, false);
        putTenantLimit(6);
        validate("app3", filesWithTwoAgents(1, 2), Map.of("app1", 2, "app2", 2), 4, true);
        putTenantLimit(4);
        validate("app3", filesWithTwoAgents(1, 2), Map.of("app1", 2, "app2", 2), 4, false);
        putTenantLimit(0);
        validate("app3", filesWithTwoAgents(1, 2), Map.of("app1", 2, "app2", 2), 4, false);
    }

    private Map<String, String> filesWithTwoAgents(int size, int parallelism) {
        return Map.of(
                "pip.yaml",
                """
                        module: mod
                        id: pip
                        resources:
                          size: %d
                          parallelism: %d
                        topics:
                          - name: "input-topic"
                            creation-mode: create-if-not-exists
                          - name: "output-topic"
                            creation-mode: create-if-not-exists
                        pipeline:
                          - id: step1
                            type: "drop"
                            input: "input-topic"
                          - id: step2
                            type: "drop"
                            output: "output-topic"
                        """
                        .formatted(size, parallelism));
    }

    @SneakyThrows
    private static void validate(
            String applicationId,
            Map<String, String> files,
            Map<String, Integer> currentUsage,
            int max,
            boolean expectValid) {
        final ApplicationService service = getApplicationService(currentUsage, max);
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
        final ExecutionPlan executionPlan =
                service.validateExecutionPlan(applicationId, application);
        try {
            service.checkResourceUsage("tenant", applicationId, executionPlan);
            if (expectValid) {
                ok = true;
            }

        } catch (IllegalArgumentException e) {
            if (!expectValid) {
                ok = true;
                log.info("Got expected exception", e);
                Assertions.assertEquals(
                        "Not enough resources to deploy application " + applicationId,
                        e.getMessage());
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

    @SneakyThrows
    private void putTenantLimit(int limit) {
        new GlobalMetadataService(
                        STORAGE_PROPERTIES,
                        new TestApplicationStore(Map.of()),
                        new TenantProperties())
                .updateTenant(
                        "tenant",
                        UpdateTenantRequest.builder().maxTotalResourceUnits(limit).build());
    }

    @NotNull
    private static ApplicationService getApplicationService(
            Map<String, Integer> currentUsage, int max) {
        final TenantProperties props = new TenantProperties();
        props.setDefaultMaxTotalResourceUnits(max);
        final TestApplicationStore applicationStore = new TestApplicationStore(currentUsage);
        return new ApplicationService(
                new GlobalMetadataService(STORAGE_PROPERTIES, applicationStore, props),
                applicationStore,
                new ApplicationDeployProperties(
                        new ApplicationDeployProperties.GatewayProperties(false)),
                props);
    }

    @AllArgsConstructor
    private static class TestApplicationStore implements ApplicationStore {
        private final Map<String, Integer> currentUsage;

        @Override
        public void validateTenant(String tenant, boolean failIfNotExists)
                throws IllegalStateException {}

        @Override
        public void onTenantCreated(String tenant) {}

        @Override
        public void onTenantDeleted(String tenant) {}

        @Override
        public void onTenantUpdated(String tenant) {}

        @Override
        public void put(
                String tenant,
                String applicationId,
                Application applicationInstance,
                String codeArchiveReference,
                ExecutionPlan executionPlan,
                boolean autoUpgrade,
                boolean forceRestart) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StoredApplication get(String tenant, String applicationId, boolean queryPods) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ApplicationSpecs getSpecs(String tenant, String applicationId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Secrets getSecrets(String tenant, String applicationId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void delete(String tenant, String applicationId, boolean force) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, StoredApplication> list(String tenant) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, Integer> getResourceUsage(String tenant) {
            return currentUsage;
        }

        @Override
        public List<PodLogHandler> logs(
                String tenant, String applicationId, LogOptions logOptions) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String storeType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void initialize(Map<String, Object> configuration) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getExecutorServiceURI(
                String tenant, String applicationId, String executorId) {
            throw new UnsupportedOperationException();
        }
    }
}
