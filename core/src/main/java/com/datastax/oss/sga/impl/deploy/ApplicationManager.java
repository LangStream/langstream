package com.datastax.oss.sga.impl.deploy;

import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.ApplicationInstanceLifecycleStatus;
import com.datastax.oss.sga.api.model.StoredApplication;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.impl.storage.ApplicationStore;
import com.google.common.util.concurrent.Striped;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class ApplicationManager implements AutoCloseable {

    private final ExecutorService executor;
    private final ApplicationDeployer applicationDeployer;
    private final ApplicationStore applicationStore;
    private final LinkedBlockingQueue<Task> tasks = new LinkedBlockingQueue<>();
    private final Striped<Lock> applicationLocks = Striped.lock(1000);

    public record Task(String tenant, String applicationName, Application applicationInstance, boolean deploy) {
    }

    public ApplicationManager(ApplicationDeployer applicationDeployer,
                              ApplicationStore applicationStore, int executors) {
        this.applicationDeployer = applicationDeployer;
        this.applicationStore = applicationStore;
        executor = Executors.newFixedThreadPool(executors);
        for (int i = 0; i < executors; i++) {
            executor.submit(this::taskRunner);
        }
    }

    private void taskRunner() {
        try {
            while (true) {
                final Task newTask = tasks.take();
                if (getLock(newTask.tenant, newTask.applicationName).tryLock(5, TimeUnit.SECONDS)) {
                    try {
                        if (newTask.deploy) {
                            internalDeploy(newTask.tenant, newTask.applicationName, newTask.applicationInstance);
                        } else {
                            internalDelete(newTask.tenant, newTask.applicationName, newTask.applicationInstance);
                        }
                    } catch (Throwable ex) {
                        log.error("Error while processing task", ex);
                    } finally {
                        getLock(newTask.tenant, newTask.applicationName).unlock();
                    }
                } else {
                    tasks.add(newTask);
                }
            }
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
        }
    }

    public void recoverTenant(String tenant) {
        final LinkedHashMap<String, StoredApplication> apps = applicationStore.list(tenant);
        recoverInFlightDeployments(tenant, apps);
        recoverInFlightUnemployments(tenant, apps);
    }

    private void recoverInFlightUnemployments(String tenant, LinkedHashMap<String, StoredApplication> apps) {
        final List<StoredApplication> deletingDeployments = apps.values()
                .stream()
                .filter(app -> app.getStatus().getStatus() == ApplicationInstanceLifecycleStatus.Status.DELETING)
                .collect(Collectors.toList());

        if (deletingDeployments.isEmpty()) {
            log.info("No inflight unemployments to recover");
        } else {
            log.info("Recovering {} inflight unemployments", deletingDeployments.size());
            for (StoredApplication deployment : deletingDeployments) {
                scheduleUnemployment(tenant, deployment.getName(), deployment.getInstance());
            }
        }
    }

    private void recoverInFlightDeployments(String tenant, LinkedHashMap<String, StoredApplication> apps) {
        final List<StoredApplication> inflightDeployments = apps.values()
                .stream()
                .filter(app -> app.getStatus().getStatus() == ApplicationInstanceLifecycleStatus.Status.CREATED)
                .collect(Collectors.toList());

        if (inflightDeployments.isEmpty()) {
            log.info("No inflight deployments to recover");
        } else {
            log.info("Recovering {} inflight deployments", inflightDeployments.size());
            for (StoredApplication inflightDeployment : inflightDeployments) {
                scheduleDeployment(tenant, inflightDeployment.getName(), inflightDeployment.getInstance());
            }
        }
    }

    @SneakyThrows
    public Map<String, StoredApplication> getAllApplications(String tenant) {
        return applicationStore.list(tenant);
    }

    @SneakyThrows
    public void deployApplication(String tenant, String applicationName, Application applicationInstance) {
        getLock(tenant, applicationName).lock();
        try {
            final StoredApplication current = applicationStore.get(tenant, applicationName);
            if (current != null) {
                throw new IllegalArgumentException("Application " + applicationName + " already exists");
            }
            applicationStore.put(tenant, applicationName, applicationInstance,
                    ApplicationInstanceLifecycleStatus.CREATED);
            scheduleDeployment(tenant, applicationName, applicationInstance);
        } finally {
            getLock(tenant, applicationName).unlock();
        }
    }

    private Lock getLock(String tenant, String applicationName) {
        return applicationLocks.get(tenant + "_" + applicationName);
    }

    private void scheduleDeployment(String tenant, String applicationName, Application applicationInstance) {
        tasks.add(new Task(tenant, applicationName, applicationInstance, true));
        log.info("Scheduled deployment of {}", applicationName);

    }

    private void scheduleUnemployment(String tenant, String applicationName, Application applicationInstance) {
        tasks.add(new Task(tenant, applicationName, applicationInstance, false));
        log.info("Scheduled unemployment of {}", applicationName);
    }

    private void internalDeploy(String tenant, String applicationName, Application applicationInstance) {
        log.info("start deploying {}", applicationName);

        try {
            final ExecutionPlan implementation =
                    applicationDeployer.createImplementation(applicationInstance);
            applicationDeployer.deploy(implementation);
            applicationStore.put(tenant, applicationName, applicationInstance,
                    ApplicationInstanceLifecycleStatus.DEPLOYED);
        } catch (Throwable e) {
            log.warn("failed to deploy {}, {}", applicationName, e.getMessage());
            applicationStore.put(tenant, applicationName, applicationInstance,
                    ApplicationInstanceLifecycleStatus.error(e.getMessage()));
            throw new RuntimeException(e);
        }
    }

    private void internalDelete(String tenant, String applicationName, Application applicationInstance) {
        log.info("start deletion of {}", applicationName);
        // not supported yet
        // deployer.delete(applicationInstance, deployer.createImplementation(current.getInstance()));
        applicationStore.delete(tenant, applicationName);
        log.info("deleted {}", applicationName);
    }

    @SneakyThrows
    public StoredApplication getApplication(String tenant, String applicationName) {
        return applicationStore.get(tenant, applicationName);
    }

    @SneakyThrows
    public void deleteApplication(String tenant, String applicationName) {
        final Lock lock = getLock(tenant, applicationName);
        lock.lock();
        try {
            final StoredApplication current = applicationStore.get(tenant, applicationName);
            if (current == null) {
                return;
            }

            boolean allowed = false;
            switch (current.getStatus().getStatus()) {
                case DEPLOYED:
                case ERROR:
                    allowed = true;
                    break;
                default:
                    break;
            }
            if (!allowed) {
                throw new IllegalArgumentException("Application " + applicationName
                        + " is not deployed, current status " + current.getStatus().getStatus());
            }

            applicationStore.put(tenant, applicationName, current.getInstance(),
                    ApplicationInstanceLifecycleStatus.DELETING);
            scheduleUnemployment(tenant, applicationName, current.getInstance());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws Exception {
        executor.shutdown();
    }
}
