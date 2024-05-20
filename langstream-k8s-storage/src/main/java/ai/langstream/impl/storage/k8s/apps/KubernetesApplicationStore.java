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
package ai.langstream.impl.storage.k8s.apps;

import ai.langstream.api.model.Application;
import ai.langstream.api.model.ApplicationSpecs;
import ai.langstream.api.model.ApplicationStatus;
import ai.langstream.api.model.Secrets;
import ai.langstream.api.model.StoredApplication;
import ai.langstream.api.runtime.ComponentType;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.storage.ApplicationStore;
import ai.langstream.deployer.k8s.agents.AgentResourcesFactory;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationSpec;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationSpecOptions;
import ai.langstream.deployer.k8s.api.crds.apps.SerializedApplicationInstance;
import ai.langstream.deployer.k8s.apps.AppResourcesFactory;
import ai.langstream.deployer.k8s.limits.ApplicationResourceLimitsChecker;
import ai.langstream.deployer.k8s.util.KubeUtil;
import ai.langstream.impl.k8s.KubernetesClientFactory;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.LogWatch;
import io.fabric8.kubernetes.client.dsl.PodResource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KubernetesApplicationStore implements ApplicationStore {

    protected static final String SECRET_KEY = "secrets";

    private static final ObjectMapper mapper =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    protected static final SimpleDateFormat UTC_RFC3339;
    protected static final SimpleDateFormat UTC_K8S_LOGS;

    static {
        UTC_RFC3339 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        UTC_RFC3339.setTimeZone(TimeZone.getTimeZone("UTC"));
        UTC_K8S_LOGS = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        UTC_K8S_LOGS.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private static final Pattern K8S_LOGS_PATTERN =
            Pattern.compile("(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{9}Z) (.*)");
    private KubernetesClient client;
    private KubernetesApplicationStoreProperties properties;

    @Override
    public String storeType() {
        return "kubernetes";
    }

    @Override
    public void initialize(Map<String, Object> configuration) {
        this.properties =
                mapper.convertValue(configuration, KubernetesApplicationStoreProperties.class);
        this.client = KubernetesClientFactory.get(null);
    }

    private String tenantToNamespace(String tenant) {
        return properties.getNamespaceprefix() + tenant;
    }

    @Override
    public void validateTenant(String tenant, boolean failIfNotExists) {
        final String namespace = tenantToNamespace(tenant);
        final Namespace ns = client.namespaces().withName(namespace).get();
        if (ns == null) {
            if (failIfNotExists) {
                log.warn("Namespace {} not found for tenant {}", namespace, tenant);
                throw new IllegalStateException("Tenant " + tenant + " does not exist");
            }
        } else if (ns.isMarkedForDeletion()) {
            throw new IllegalStateException("Tenant " + tenant + " is marked for deletion.");
        }
    }

    @Override
    @SneakyThrows
    public void onTenantCreated(String tenant) {
        final String namespace = tenantToNamespace(tenant);
        new TenantResources(properties, client, tenant, namespace).ensureTenantResources();
    }

    @Override
    public void onTenantUpdated(String tenant) {}

    @Override
    public void onTenantDeleted(String tenant) {
        final String namespace = tenantToNamespace(tenant);
        if (client.namespaces().withName(namespace).get() != null) {
            client.namespaces().withName(namespace).delete();
        }
    }

    @Override
    @SneakyThrows
    public void put(
            String tenant,
            String applicationId,
            Application applicationInstance,
            String codeArchiveId,
            ExecutionPlan executionPlan,
            boolean autoUpgrade,
            boolean forceRestart) {
        final ApplicationCustomResource existing =
                getApplicationCustomResource(tenant, applicationId);
        final ApplicationSpecOptions existingOptions =
                existing != null
                        ? ApplicationSpec.deserializeOptions(existing.getSpec().getOptions())
                        : null;
        if (existing != null) {
            if (existing.isMarkedForDeletion()) {
                throw new IllegalArgumentException(
                        "Application " + applicationId + " is marked for deletion.");
            }
            if (existingOptions.isMarkedForDeletion()) {
                throw new IllegalArgumentException(
                        "Application " + applicationId + " is marked for deletion.");
            }
        }

        final String namespace = tenantToNamespace(tenant);

        ApplicationCustomResource crd = new ApplicationCustomResource();
        crd.setMetadata(
                new ObjectMetaBuilder().withName(applicationId).withNamespace(namespace).build());
        final SerializedApplicationInstance serializedApp =
                new SerializedApplicationInstance(applicationInstance, executionPlan);

        final ApplicationSpec spec = new ApplicationSpec();
        spec.setTenant(tenant);
        spec.setApplication(ApplicationSpec.serializeApplication(serializedApp));
        spec.setCodeArchiveId(codeArchiveId);
        ApplicationSpecOptions specOptions = new ApplicationSpecOptions();
        specOptions.setAutoUpgradeRuntimeImage(autoUpgrade);
        specOptions.setAutoUpgradeRuntimeImagePullPolicy(autoUpgrade);
        specOptions.setAutoUpgradeAgentResources(autoUpgrade);
        specOptions.setAutoUpgradeAgentPodTemplate(autoUpgrade);
        if (forceRestart) {
            specOptions.setSeed(System.nanoTime());
        } else {
            // very important to not overwrite the seed if we are not forcing a restart
            if (existingOptions != null) {
                specOptions.setSeed(existingOptions.getSeed());
            } else {
                specOptions.setSeed(0L);
            }
        }
        spec.setOptions(ApplicationSpec.serializeOptions(specOptions));

        log.info(
                "Creating application {} in namespace {}, spec {}", applicationId, namespace, spec);
        crd.setSpec(spec);

        client.resource(crd).inNamespace(namespace).serverSideApply();

        // need to refresh to get the uid
        crd = client.resource(crd).inNamespace(namespace).get();

        final Secret secret =
                new SecretBuilder()
                        .withNewMetadata()
                        .withName(applicationId)
                        .withNamespace(namespace)
                        .withOwnerReferences(KubeUtil.getOwnerReferenceForResource(crd))
                        .endMetadata()
                        .withData(
                                Map.of(
                                        SECRET_KEY,
                                        encodeSecret(
                                                mapper.writeValueAsString(
                                                        applicationInstance.getSecrets()))))
                        .build();
        client.resource(secret).inNamespace(namespace).serverSideApply();
    }

    @Override
    public StoredApplication get(String tenant, String applicationId, boolean queryPods) {
        final ApplicationCustomResource application =
                getApplicationCustomResource(tenant, applicationId);
        if (application == null) {
            return null;
        }
        return getApplicationStatus(applicationId, application, queryPods);
    }

    @Override
    public ApplicationSpecs getSpecs(String tenant, String applicationId) {
        final ApplicationCustomResource application =
                getApplicationCustomResource(tenant, applicationId);
        if (application == null) {
            return null;
        }
        SerializedApplicationInstance serializedApplicationInstanceFromCr =
                getSerializedApplicationInstanceFromCr(application);
        final Application app = serializedApplicationInstanceFromCr.toApplicationInstance();
        return ApplicationSpecs.builder()
                .application(app)
                .codeArchiveReference(application.getSpec().getCodeArchiveId())
                .applicationId(applicationId)
                .build();
    }

    @Override
    @SneakyThrows
    public Secrets getSecrets(String tenant, String applicationId) {
        final Secret secret =
                client.secrets()
                        .inNamespace(tenantToNamespace(tenant))
                        .withName(applicationId)
                        .get();
        if (secret == null) {
            return null;
        }

        if (secret.getData() != null && secret.getData().get(SECRET_KEY) != null) {
            final String s = secret.getData().get(SECRET_KEY);
            final String decoded =
                    new String(Base64.getDecoder().decode(s), StandardCharsets.UTF_8);
            return mapper.readValue(decoded, Secrets.class);
        }
        return null;
    }

    private ApplicationCustomResource getApplicationCustomResource(
            String tenant, String applicationId) {
        final String namespace = tenantToNamespace(tenant);
        return client.resources(ApplicationCustomResource.class)
                .inNamespace(namespace)
                .withName(applicationId)
                .get();
    }

    @Override
    public void delete(String tenant, String applicationId, boolean force) {
        final ApplicationCustomResource existing =
                getApplicationCustomResource(tenant, applicationId);
        if (existing == null || existing.isMarkedForDeletion()) {
            return;
        }
        final ApplicationSpecOptions options =
                ApplicationSpec.deserializeOptions(existing.getSpec().getOptions());

        boolean apply = false;
        if (!options.isMarkedForDeletion()) {
            options.setMarkedForDeletion(true);
            apply = true;
        }

        if (options.getDeleteMode() == ApplicationSpecOptions.DeleteMode.CLEANUP_REQUIRED
                && force) {
            options.setDeleteMode(ApplicationSpecOptions.DeleteMode.CLEANUP_BEST_EFFORT);
            apply = true;
        }
        if (apply) {
            existing.getSpec().setOptions(ApplicationSpec.serializeOptions(options));
            client.resource(existing).serverSideApply();
        }
        // the secret deletion will happen automatically once the app custom resource has been
        // deleted completely
    }

    @Override
    public Map<String, StoredApplication> list(String tenant) {
        final String namespace = tenantToNamespace(tenant);
        return client
                .resources(ApplicationCustomResource.class)
                .inNamespace(namespace)
                .list()
                .getItems()
                .stream()
                .map(a -> getApplicationStatus(a.getMetadata().getName(), a, false))
                .collect(
                        Collectors.toMap(StoredApplication::getApplicationId, Function.identity()));
    }

    @Override
    public Map<String, Integer> getResourceUsage(String tenant) {
        final String namespace = tenantToNamespace(tenant);
        return ApplicationResourceLimitsChecker.loadUsage(client, namespace);
    }

    @SneakyThrows
    private StoredApplication getApplicationStatus(
            String applicationId, ApplicationCustomResource application, boolean queryPods) {
        SerializedApplicationInstance serializedApplicationInstanceFromCr =
                getSerializedApplicationInstanceFromCr(application);
        final Application instance = serializedApplicationInstanceFromCr.toApplicationInstance();
        final Map<String, SerializedApplicationInstance.AgentRunnerDefinition> agentNodes =
                serializedApplicationInstanceFromCr.getAgentRunners();

        return StoredApplication.builder()
                .applicationId(applicationId)
                .instance(instance)
                .codeArchiveReference(application.getSpec().getCodeArchiveId())
                .status(computeApplicationStatus(applicationId, agentNodes, application, queryPods))
                .build();
    }

    private SerializedApplicationInstance getSerializedApplicationInstanceFromCr(
            ApplicationCustomResource application) {
        return ApplicationSpec.deserializeApplication(application.getSpec().getApplication());
    }

    private ApplicationStatus computeApplicationStatus(
            final String applicationId,
            Map<String, SerializedApplicationInstance.AgentRunnerDefinition> agentRunners,
            ApplicationCustomResource customResource,
            boolean queryPods) {
        log.info(
                "Computing status for application {}, agentRunners {}",
                applicationId,
                agentRunners);
        final ApplicationStatus result = new ApplicationStatus();
        result.setStatus(AppResourcesFactory.computeApplicationStatus(client, customResource));
        List<String> declaredAgents = new ArrayList<>();

        Map<String, AgentResourcesFactory.AgentRunnerSpec> agentRunnerSpecMap = new HashMap<>();
        agentRunners.forEach(
                (agentId, agentRunnerDefinition) -> {
                    log.info("Adding agent id {} (def {})", agentId, agentRunnerDefinition);
                    // this agentId doesn't contain the "module" prefix
                    declaredAgents.add(agentRunnerDefinition.getAgentId());
                    agentRunnerSpecMap.put(
                            agentId,
                            AgentResourcesFactory.AgentRunnerSpec.builder()
                                    .agentId(agentRunnerDefinition.getAgentId())
                                    .agentType(agentRunnerDefinition.getAgentType())
                                    .componentType(agentRunnerDefinition.getComponentType())
                                    .configuration(agentRunnerDefinition.getConfiguration())
                                    .inputTopic(agentRunnerDefinition.getInputTopic())
                                    .outputTopic(agentRunnerDefinition.getOutputTopic())
                                    .build());
                });

        result.setAgents(
                AgentResourcesFactory.aggregateAgentsStatus(
                        client,
                        customResource.getMetadata().getNamespace(),
                        applicationId,
                        declaredAgents,
                        agentRunnerSpecMap,
                        queryPods));
        return result;
    }

    static String encodeSecret(String value) {
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    @SneakyThrows
    public List<PodLogHandler> logs(String tenant, String applicationId, LogOptions logOptions) {
        final List<String> pods =
                AgentResourcesFactory.getAgentPods(
                        client, tenantToNamespace(tenant), applicationId);

        return pods.stream()
                .filter(
                        pod -> {
                            if (logOptions.getFilterReplicas() != null
                                    && !logOptions.getFilterReplicas().isEmpty()) {
                                return logOptions.getFilterReplicas().contains(pod);
                            }
                            return true;
                        })
                .map(pod -> new StreamPodLogHandler(tenant, pod))
                .collect(Collectors.toList());
    }

    private class StreamPodLogHandler implements PodLogHandler {
        private final String tenant;
        private final String pod;
        private InputStream in;
        private volatile boolean closed;

        public StreamPodLogHandler(String tenant, String pod) {
            this.tenant = tenant;
            this.pod = pod;
        }

        @Override
        public String getPodName() {
            return pod;
        }

        private record StreamLogResult(LogLineResult logLineResult, boolean logged) {}

        @Override
        @SneakyThrows
        public void start(LogLineConsumer onLogLine) {
            Long sinceTime = null;
            while (true) {
                try {

                    final StreamLogResult streamLogResult = streamUntilEOF(onLogLine, sinceTime);
                    if (streamLogResult.logged()) {
                        sinceTime = System.currentTimeMillis();
                    }
                    final LogLineResult result = streamLogResult.logLineResult();
                    if (!result.continueLogging()) {
                        return;
                    }
                    if (closed) {
                        return;
                    }
                    if (result.delayInSeconds() != null && result.delayInSeconds() > 0) {
                        Thread.sleep(result.delayInSeconds() * 1000);
                    }
                } catch (Throwable tt) {
                    if (!closed) {
                        log.warn("Error while streaming logs for pod {}", pod, tt);
                    }
                    onLogLine.onEnd();
                }
            }
        }

        @Override
        public void close() {
            closed = true;
            try {
                in.close();
            } catch (Throwable ignored) {
            }
        }

        private StreamLogResult streamUntilEOF(LogLineConsumer onLogLine, Long sinceTime)
                throws IOException {
            final PodResource podResource =
                    client.pods().inNamespace(tenantToNamespace(tenant)).withName(pod);

            final Pod pod = podResource.get();
            if (pod == null) {
                return new StreamLogResult(
                        onLogLine.onPodNotRunning(
                                "NotFound",
                                "Not found, probably the application is still not completely deployed."),
                        false);
            }
            final Map<String, KubeUtil.PodStatus> status = KubeUtil.getPodsStatuses(List.of(pod));
            final KubeUtil.PodStatus podStatus = status.values().iterator().next();
            if (podStatus.getState() != KubeUtil.PodStatus.State.RUNNING) {
                return new StreamLogResult(
                        onLogLine.onPodNotRunning(
                                podStatus.getState().toString(), podStatus.getMessage()),
                        false);
            }

            final String sinceTimeString;
            if (sinceTime != null) {
                final Date date = new Date();
                date.setTime(sinceTime);
                sinceTimeString = UTC_RFC3339.format(date);
            } else {
                sinceTimeString = null;
            }
            try (final LogWatch watchLog =
                    podResource
                            .inContainer("runtime")
                            .usingTimestamps()
                            .sinceTime(sinceTimeString)
                            .withPrettyOutput()
                            .withReadyWaitTimeout(Integer.MAX_VALUE)
                            .watchLog(); ) {
                in = watchLog.getOutput();
                try (BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String logContent;
                        long timestamp;

                        Matcher m = K8S_LOGS_PATTERN.matcher(line);
                        if (m.find()) {
                            String timestampString = m.group(1);
                            try {
                                timestamp = UTC_K8S_LOGS.parse(timestampString).getTime();
                            } catch (ParseException e) {
                                timestamp = 0;
                            }
                            logContent = m.group(2);
                        } else {
                            timestamp = 0;
                            logContent = line;
                        }

                        final LogLineResult logLineResult =
                                onLogLine.onLogLine(logContent, timestamp);
                        if (!logLineResult.continueLogging()) {
                            return new StreamLogResult(logLineResult, true);
                        }
                        if (closed) {
                            return new StreamLogResult(new LogLineResult(false, null), true);
                        }
                    }
                }
            }
            return new StreamLogResult(onLogLine.onPodLogNotAvailable(), true);
        }
    }

    @Override
    public String getExecutorServiceURI(String tenant, String applicationId, String executorId) {
        final ApplicationCustomResource application =
                getApplicationCustomResource(tenant, applicationId);
        if (application == null) {
            return null;
        }
        SerializedApplicationInstance serializedApplicationInstanceFromCr =
                getSerializedApplicationInstanceFromCr(application);
        final SerializedApplicationInstance.AgentRunnerDefinition agentId =
                serializedApplicationInstanceFromCr.getAgentRunners().values().stream()
                        .filter(a -> a.getAgentId().equals(executorId))
                        .findFirst()
                        .orElse(null);
        if (agentId == null) {
            throw new IllegalArgumentException(
                    "Executor "
                            + executorId
                            + " not found. Ensure the agent is of type 'service' and not composite.");
        }
        if (!ComponentType.SERVICE.equals(ComponentType.valueOf(agentId.getComponentType()))) {
            throw new IllegalArgumentException(
                    "Executor " + executorId + " is not of type 'service'.");
        }
        // avoid Service looks up for performance
        final String namespace = tenantToNamespace(tenant);
        final String svcName =
                AgentResourcesFactory.getAgentCustomResourceName(
                        applicationId, agentId.getAgentId());
        return KubeUtil.computeServiceUrl(svcName, namespace, 8000);
    }
}
