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
package ai.langstream.deployer.k8s.limits;

import static org.junit.jupiter.api.Assertions.*;

import ai.langstream.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import ai.langstream.deployer.k8s.util.KeyedLockHandler;
import ai.langstream.deployer.k8s.util.SerializationUtil;
import ai.langstream.impl.k8s.tests.KubeK3sServer;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class ApplicationResourceLimitsCheckerTest {
    @RegisterExtension static final KubeK3sServer k3s = new KubeK3sServer(true);

    @Test
    void testLimits() {
        final ApplicationResourceLimitsChecker checker =
                new ApplicationResourceLimitsChecker(
                        k3s.getClient(), new KeyedLockHandler(), tenant -> 10);

        final String tenant1 = genTenant();
        final String tenant2 = genTenant();
        setupTenant(tenant1);
        setupTenant(tenant2);
        // must not be kept in consideration
        createApp(tenant2, 10, 1);

        ApplicationCustomResource app1 = createApp(tenant1, 10, 1);
        assertTrue(checker.checkLimitsForTenant(app1));
        deleteApp(app1, checker);

        app1 = createApp(tenant1, 1, 10);
        assertTrue(checker.checkLimitsForTenant(app1));

        ApplicationCustomResource app2 = createApp(tenant1, 1, 1);
        assertFalse(checker.checkLimitsForTenant(app2));

        deleteApp(app1, checker);
        assertTrue(checker.checkLimitsForTenant(app2));
    }

    @Test
    void testConcurrency() {
        final ApplicationResourceLimitsChecker checker =
                new ApplicationResourceLimitsChecker(
                        k3s.getClient(), new KeyedLockHandler(), tenant -> 10);

        final String tenant1 = genTenant();
        final String tenant2 = genTenant();
        setupTenant(tenant1);
        setupTenant(tenant2);

        final int nThreads = 50;
        final ExecutorService executorService = Executors.newFixedThreadPool(nThreads);

        AtomicInteger countDone = new AtomicInteger();
        AtomicReference<String> successfullApp = new AtomicReference();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < nThreads; i++) {
            final CompletableFuture<Void> future =
                    CompletableFuture.runAsync(
                            () -> {
                                ApplicationCustomResource app1 = createApp(tenant1, 10, 1);
                                final boolean done = checker.checkLimitsForTenant(app1);
                                if (done) {
                                    countDone.incrementAndGet();
                                    successfullApp.set(app1.getMetadata().getName());
                                }
                            },
                            executorService);
            futures.add(future);
        }
        futures.forEach(CompletableFuture::join);
        assertEquals(1, countDone.get());
        ApplicationCustomResource anotherApp = createApp(tenant1, 1, 1);

        ApplicationCustomResource successfullAppUpgrade = createApp(tenant1, 1, 9);
        successfullAppUpgrade.getMetadata().setName(successfullApp.get());
        assertTrue(checker.checkLimitsForTenant(successfullAppUpgrade));
        assertTrue(checker.checkLimitsForTenant(anotherApp));
        assertFalse(checker.checkLimitsForTenant(createApp(tenant1, 1, 1)));

        checker.onAppBeingDeleted(successfullAppUpgrade);
        assertTrue(checker.checkLimitsForTenant(createApp(tenant1, 9, 1)));
    }

    private void deleteApp(
            ApplicationCustomResource resource, ApplicationResourceLimitsChecker checker) {
        k3s.getClient().resource(resource).delete();
        checker.onAppBeingDeleted(resource);
    }

    private ApplicationCustomResource createApp(String tenant, int size, int parallelism) {
        final String appId = genAppId();
        ApplicationCustomResource resource =
                getCr(
                        """
                                apiVersion: langstream.ai/v1alpha1
                                kind: Application
                                metadata:
                                  name: %s
                                  namespace: %s
                                spec:
                                    application: '{"agentRunners": {"agent1": {"resources": {"size": %d, "parallelism": %d}}}}'
                                    tenant: %s
                                """
                                .formatted(appId, tenant, size, parallelism, tenant));
        return resource;
    }

    static AtomicInteger counter = new AtomicInteger(0);

    private String genAppId() {
        return "app-%s".formatted(counter.incrementAndGet());
    }

    private String genTenant() {
        return "tenant-%s".formatted(counter.incrementAndGet());
    }

    private ApplicationCustomResource getCr(String yaml) {
        return SerializationUtil.readYaml(yaml, ApplicationCustomResource.class);
    }

    private void setupTenant(String tenant) {
        k3s.getClient()
                .resource(
                        new NamespaceBuilder()
                                .withNewMetadata()
                                .withName(tenant)
                                .endMetadata()
                                .build())
                .create();
    }
}
