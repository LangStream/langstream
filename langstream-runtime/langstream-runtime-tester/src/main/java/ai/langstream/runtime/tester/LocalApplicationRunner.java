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
package ai.langstream.runtime.tester;

import ai.langstream.api.model.Application;
import ai.langstream.api.runner.assets.AssetManagerRegistry;
import ai.langstream.api.runner.code.MetricsReporter;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeRegistry;
import ai.langstream.api.runtime.*;
import ai.langstream.deployer.k8s.agents.AgentResourcesFactory;
import ai.langstream.impl.deploy.ApplicationDeployer;
import ai.langstream.impl.nar.NarFileHandler;
import ai.langstream.impl.parser.ModelBuilder;
import ai.langstream.runtime.agent.AgentRunner;
import ai.langstream.runtime.agent.api.AgentAPIController;
import ai.langstream.runtime.agent.metrics.PrometheusMetricsReporter;
import ai.langstream.runtime.api.agent.RuntimePodConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.fabric8.kubernetes.api.model.Secret;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

@Slf4j
public class LocalApplicationRunner
        implements AutoCloseable, InMemoryApplicationStore.AgentInfoCollector {

    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

    final KubeTestServer kubeServer = new KubeTestServer();
    final InMemoryApplicationStore applicationStore = new InMemoryApplicationStore();
    final ApplicationDeployer applicationDeployer;
    final NarFileHandler narFileHandler;

    final Path agentsDirectory;

    final Path codeDirectory;

    final Path basePersistentStateDirectory;

    final AtomicBoolean continueLoop = new AtomicBoolean(true);

    final CountDownLatch exited = new CountDownLatch(1);

    final AtomicBoolean started = new AtomicBoolean();

    final Map<String, AgentAPIController> allAgentsInfo = new ConcurrentHashMap<>();

    public LocalApplicationRunner(
            Path agentsDirectory, Path codeDirectory, Path basePersistentStateDirectory)
            throws Exception {
        this.codeDirectory = codeDirectory;
        this.agentsDirectory = agentsDirectory;
        this.basePersistentStateDirectory = basePersistentStateDirectory;
        List<URL> customLib = AgentRunner.buildCustomLibClasspath(codeDirectory);
        this.narFileHandler =
                new NarFileHandler(
                        agentsDirectory, customLib, Thread.currentThread().getContextClassLoader());
        TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry =
                new TopicConnectionsRuntimeRegistry();
        narFileHandler.scan();
        topicConnectionsRuntimeRegistry.setPackageLoader(narFileHandler);
        AssetManagerRegistry assetManagerRegistry = new AssetManagerRegistry();
        assetManagerRegistry.setAssetManagerPackageLoader(narFileHandler);
        this.applicationDeployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .pluginsRegistry(new PluginsRegistry())
                        .topicConnectionsRuntimeRegistry(topicConnectionsRuntimeRegistry)
                        .assetManagerRegistry(assetManagerRegistry)
                        .deployContext(DeployContext.NO_DEPLOY_CONTEXT)
                        .build();
    }

    protected record ApplicationRuntime(
            String tenant,
            String applicationId,
            Application applicationInstance,
            ExecutionPlan implementation,
            Map<String, Secret> secrets,
            ApplicationDeployer applicationDeployer)
            implements AutoCloseable {

        public <T> T getGlobal(String key) {
            return (T) implementation.getApplication().getInstance().globals().get(key);
        }

        public void close() {
            applicationDeployer.delete(tenant, implementation, null);
        }
    }

    public ApplicationRuntime deployApplicationWithSecrets(
            String tenant,
            String appId,
            ModelBuilder.ApplicationWithPackageInfo applicationWithPackageInfo,
            String... agents)
            throws Exception {

        kubeServer.spyAgentCustomResources(tenant, agents);
        final Map<String, Secret> secrets =
                kubeServer.spyAgentCustomResourcesSecrets(tenant, agents);

        Application applicationInstance = applicationWithPackageInfo.getApplication();

        ExecutionPlan implementation =
                applicationDeployer.createImplementation(appId, applicationInstance);

        ensureDiskDirectories(implementation);

        applicationDeployer.setup(tenant, implementation);

        applicationDeployer.deploy(tenant, implementation, null);

        applicationStore.put(
                tenant,
                appId,
                applicationInstance,
                "no-code-archive-reference",
                implementation,
                false,
                false);

        return new ApplicationRuntime(
                tenant, appId, applicationInstance, implementation, secrets, applicationDeployer);
    }

    private void ensureDiskDirectories(ExecutionPlan implementation) throws IOException {
        for (AgentNode value : implementation.getAgents().values()) {
            if (value.getDisks() != null) {
                for (String s : value.getDisks().keySet()) {
                    Files.createDirectories(basePersistentStateDirectory.resolve(s));
                }
            }
        }
    }

    @Override
    public Map<String, AgentAPIController> collectAgentsStatus() {
        return new HashMap<>(allAgentsInfo);
    }

    public record AgentRunResult(Map<String, AgentAPIController> info) {}

    public void start() {
        kubeServer.start();
    }

    public AgentRunResult executeAgentRunners(ApplicationRuntime runtime, List<String> agents)
            throws Exception {

        String runnerExecutionId = UUID.randomUUID().toString();
        log.info(
                "{} Starting Agent Runners. Running {} pods",
                runnerExecutionId,
                runtime.secrets.size());

        started.set(true);
        try {
            List<RuntimePodConfiguration> pods = new ArrayList<>();
            runtime.secrets()
                    .forEach(
                            (key, secret) -> {
                                if (agents.contains(key)) {
                                    RuntimePodConfiguration runtimePodConfiguration =
                                            AgentResourcesFactory
                                                    .readRuntimePodConfigurationFromSecret(secret);
                                    if (log.isDebugEnabled()) {
                                        log.debug(
                                                "{} Pod configuration {} = {}",
                                                runnerExecutionId,
                                                key,
                                                runtimePodConfiguration);
                                    }
                                    pods.add(runtimePodConfiguration);
                                } else {
                                    log.info("Agent {} won't be executed", key);
                                }
                            });

            MetricsReporter globalMetricsReporter = new PrometheusMetricsReporter();

            // execute all the pods
            ExecutorService executorService = Executors.newCachedThreadPool();
            List<CompletableFuture<?>> futures = new ArrayList<>();
            for (RuntimePodConfiguration podConfiguration : pods) {
                persistPodConfiguration(podConfiguration);
                CompletableFuture<?> handle = new CompletableFuture<>();
                futures.add(handle);
                executorService.submit(
                        () -> {
                            MetricsReporter metricsReporter =
                                    globalMetricsReporter.withPodName(
                                            podConfiguration.agent().agentId());
                            String originalName = Thread.currentThread().getName();
                            Thread.currentThread()
                                    .setName(
                                            podConfiguration.agent().agentId()
                                                    + "-runner-"
                                                    + runnerExecutionId);
                            try {
                                log.info(
                                        "{} AgentPod {} Started",
                                        runnerExecutionId,
                                        podConfiguration.agent().agentId());
                                AgentAPIController agentAPIController = new AgentAPIController();
                                allAgentsInfo.put(
                                        podConfiguration.agent().agentId(), agentAPIController);
                                AgentRunner.runAgent(
                                        podConfiguration,
                                        codeDirectory,
                                        agentsDirectory,
                                        basePersistentStateDirectory,
                                        agentAPIController,
                                        continueLoop::get,
                                        () -> {},
                                        false,
                                        narFileHandler,
                                        metricsReporter);
                                List<?> infos = agentAPIController.serveWorkerStatus();
                                log.info(
                                        "{} AgentPod {} AgentInfo {}",
                                        runnerExecutionId,
                                        podConfiguration.agent().agentId(),
                                        infos);
                                handle.complete(null);
                            } catch (Throwable error) {
                                log.error(
                                        "{} Error on AgentPod {}",
                                        runnerExecutionId,
                                        podConfiguration.agent().agentId(),
                                        error);
                                handle.completeExceptionally(error);
                            } finally {
                                log.info(
                                        "{} AgentPod {} finished",
                                        runnerExecutionId,
                                        podConfiguration.agent().agentId());
                                Thread.currentThread().setName(originalName);
                            }
                        });
            }
            try {
                CompletionWaiter waiter = new CompletionWaiter(futures);
                waiter.awaitCompletion(Integer.MAX_VALUE, TimeUnit.SECONDS);
            } catch (Throwable executionException) {
                log.error(
                        "Some error occurred while executing the agent",
                        executionException.getCause());
                // unwrap the exception in order to easily perform assertions
                if (executionException instanceof RuntimeException re) {
                    throw re;
                }
                if (executionException instanceof Exception e) {
                    throw e;
                } else {
                    throw new RuntimeException(executionException);
                }
            }
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.MINUTES);
        } finally {
            log.info("{} Agent Runners Stopped", runnerExecutionId);
            exited.countDown();
        }
        return new AgentRunResult(allAgentsInfo);
    }

    @NotNull
    private static Path persistPodConfiguration(RuntimePodConfiguration podConfiguration)
            throws IOException {
        Path podRuntimeConfigurationFile = Files.createTempFile("podruntime", ".yaml");
        try (OutputStream out = Files.newOutputStream(podRuntimeConfigurationFile)) {
            MAPPER.writeValue(out, podConfiguration);
        }
        return podRuntimeConfigurationFile;
    }

    public void close() {
        continueLoop.set(false);

        if (started.get()) {
            try {
                exited.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException ok) {
            }
        }
        if (applicationDeployer != null) {
            // this closes the kubernetes client
            applicationDeployer.close();
        }
        if (narFileHandler != null) {
            narFileHandler.close();
        }
    }
}
