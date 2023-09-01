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

import ai.langstream.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationSpec;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationStatus;
import ai.langstream.deployer.k8s.api.crds.apps.SerializedApplicationInstance;
import ai.langstream.deployer.k8s.util.KeyedLockHandler;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Slf4j
public class ApplicationResourceLimitsChecker {

    private final KubernetesClient client;
    private final KeyedLockHandler lockHandler;
    private final Function<String, Integer> limitSupplier;
    private final Map<String, Map<String, Integer>> tenantUsage = new ConcurrentHashMap<>();

    public boolean checkLimitsForTenant(ApplicationCustomResource applicationCustomResource) {

        final String tenant = applicationCustomResource.getSpec().getTenant();
        final String namespace = applicationCustomResource.getMetadata().getNamespace();
        final String applicationId = applicationCustomResource.getMetadata().getName();
        final int requestedUnits = computeRequestedUnits(applicationCustomResource);
        if (requestedUnits <= 0) {
            log.warn(
                    "Unknown resource requests for application {}/{}, will deploy anyway.",
                    tenant,
                    applicationId);
            return true;
        }
        log.info("Application {}/{} requires {} units", tenant, applicationId, requestedUnits);

        try (KeyedLockHandler.LockHolder lock = lockHandler.lock(tenant)) {
            log.info("Checking limits for application {}/{}", tenant, applicationId);
            final Integer max = limitSupplier.apply(tenant);
            if (max == null || max <= 0) {
                log.info("No limit for tenant {}", tenant);
                return true;
            }

            final Map<String, Integer> actualTenantUsageByApp =
                    tenantUsage.computeIfAbsent(tenant, ignore -> loadUsage(namespace));

            final Integer currentUsage =
                    actualTenantUsageByApp.entrySet().stream()
                            .filter(e -> !e.getKey().equals(applicationId))
                            .collect(Collectors.summingInt(e -> e.getValue()));

            log.info(
                    "Current usage for tenant {} is {}/{} units (excluding application {}), distribution: {}",
                    tenant,
                    currentUsage,
                    max,
                    applicationId,
                    actualTenantUsageByApp);

            final int totalUnits = currentUsage + requestedUnits;

            if (max >= totalUnits) {
                log.info(
                        "Accept to deploy application {} with {} units",
                        applicationId,
                        requestedUnits);
                actualTenantUsageByApp.put(applicationId, requestedUnits);
                return true;
            } else {
                log.info(
                        "Reject to deploy application {} with {} units",
                        applicationId,
                        requestedUnits);
                return false;
            }
        }
    }

    public void onAppBeingDeleted(ApplicationCustomResource applicationCustomResource) {
        final String tenant = applicationCustomResource.getSpec().getTenant();
        final String namespace = applicationCustomResource.getMetadata().getNamespace();
        final String applicationId = applicationCustomResource.getMetadata().getName();

        try (KeyedLockHandler.LockHolder lock = lockHandler.lock(tenant)) {
            final Map<String, Integer> actualTenantUsageByApp =
                    tenantUsage.computeIfAbsent(tenant, ignore -> loadUsage(namespace));
            if (actualTenantUsageByApp != null) {
                actualTenantUsageByApp.remove(applicationId);
            }
        }
    }

    private Map<String, Integer> loadUsage(String namespace) {
        return loadUsage(client, namespace);
    }

    public static Map<String, Integer> loadUsage(KubernetesClient client, String namespace) {
        final List<ApplicationCustomResource> applications =
                client.resources(ApplicationCustomResource.class)
                        .inNamespace(namespace)
                        .list()
                        .getItems();
        if (applications.isEmpty()) {
            return new HashMap<>();
        }
        Map<String, Integer> appUsage = new HashMap<>();
        for (ApplicationCustomResource application : applications) {
            if (application.isMarkedForDeletion()) {
                continue;
            }
            if (application.getStatus() != null) {
                final ApplicationStatus.ResourceLimitStatus resourceLimitStatus =
                        application.getStatus().getResourceLimitStatus();
                if (resourceLimitStatus != null
                        && resourceLimitStatus == ApplicationStatus.ResourceLimitStatus.REJECTED) {
                    continue;
                }
            }
            final int usage = computeRequestedUnits(application);
            if (usage <= 0) {
                log.warn(
                        "Unknown resource requests for application {}/{}. This app won't be counted for tenant "
                                + "resource usage.",
                        application.getSpec().getTenant(),
                        application.getMetadata().getName());
                continue;
            }
            appUsage.put(application.getMetadata().getName(), usage);
        }
        return appUsage;
    }

    public static int computeRequestedUnits(ApplicationCustomResource applicationCustomResource) {

        final SerializedApplicationInstance serializedApplicationInstance =
                ApplicationSpec.deserializeApplication(
                        applicationCustomResource.getSpec().getApplication());
        final Map<String, SerializedApplicationInstance.AgentRunnerDefinition> runners =
                serializedApplicationInstance.getAgentRunners();
        if (runners == null || runners.isEmpty()) {
            log.warn(
                    "Application {} has no agents configured, this might be a old format of the agent definition.",
                    applicationCustomResource.getMetadata().getName());
            return -1;
        }

        int totalUnits = 0;
        for (SerializedApplicationInstance.AgentRunnerDefinition agent : runners.values()) {
            if (agent.getResources() == null) {
                // mantain backward-compatibility
                log.warn(
                        "Found agent {} with null resources, this might be a old format of the agent definition.",
                        agent.getAgentId(),
                        agent.getResources());
                return -1;
            }
            totalUnits += agent.getResources().parallelism() * agent.getResources().size();
        }
        return totalUnits;
    }
}
