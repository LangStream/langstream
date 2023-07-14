package com.datastax.oss.sga.deployer.k8s.controllers;

import com.datastax.oss.sga.deployer.k8s.DeployerConfiguration;
import com.datastax.oss.sga.deployer.k8s.api.crds.BaseStatus;
import com.datastax.oss.sga.deployer.k8s.util.SerializationUtil;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Cleaner;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import jakarta.inject.Inject;
import java.util.concurrent.TimeUnit;
import lombok.extern.jbosslog.JBossLog;

@JBossLog
public abstract class BaseController<T extends CustomResource> implements Reconciler<T>,
        Cleaner<T> {

    @Inject
    protected KubernetesClient client;

    @Inject
    protected DeployerConfiguration configuration;

    protected abstract UpdateControl<T> patchResources(T resource, Context<T> context);

    protected abstract DeleteControl cleanupResources(T resource, Context<T> context);

    @Override
    public DeleteControl cleanup(T resource, Context<T> context) {
        DeleteControl result;
        try {
            result = cleanupResources(resource, context);
        } catch (Throwable throwable) {
            log.errorf(throwable, "Error during cleanup for resource %s with name %s: %s",
                    resource.getFullResourceName(),
                    resource.getMetadata().getName(),
                    throwable.getMessage());
            result = DeleteControl.defaultDelete()
                    .rescheduleAfter(5, TimeUnit.SECONDS);
        }
        return result;
    }

    @Override
    public UpdateControl<T> reconcile(T resource, Context<T> context) throws Exception {
        String lastApplied;
        UpdateControl<T> result;
        final BaseStatus baseStatus = new BaseStatus();
        try {
            result = patchResources(resource, context);
            lastApplied = SerializationUtil.writeAsJson(resource.getSpec());
            baseStatus.setLastApplied(lastApplied);

        } catch (Throwable throwable) {
            log.errorf(throwable, "Error during reconciliation for resource %s with name %s: %s",
                    resource.getFullResourceName(),
                    resource.getMetadata().getName(),
                    throwable.getMessage());
            result = UpdateControl.updateResource(resource)
                    .rescheduleAfter(5, TimeUnit.SECONDS);
        }
        resource.setStatus(baseStatus);
        return result;
    }
}
