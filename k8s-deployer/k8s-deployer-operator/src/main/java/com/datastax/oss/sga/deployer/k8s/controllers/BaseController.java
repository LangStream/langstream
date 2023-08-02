/**
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
package com.datastax.oss.sga.deployer.k8s.controllers;

import com.datastax.oss.sga.deployer.k8s.DeployerConfiguration;
import com.datastax.oss.sga.deployer.k8s.ResolvedDeployerConfiguration;
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
public abstract class BaseController<T extends CustomResource<?, ? extends BaseStatus>> implements Reconciler<T>,
        Cleaner<T> {

    @Inject
    protected KubernetesClient client;

    @Inject
    protected ResolvedDeployerConfiguration configuration;

    protected abstract UpdateControl<T> patchResources(T resource, Context<T> context);

    protected abstract DeleteControl cleanupResources(T resource, Context<T> context);

    @Override
    public DeleteControl cleanup(T resource, Context<T> context) {
        DeleteControl result;
        try {
            result = cleanupResources(resource, context);
            log.infof("Reconcilied cleanup for application %s, reschedule: %s, status: %s",
                    resource.getMetadata().getName(),
                    String.valueOf(result.getScheduleDelay().isPresent()),
                    resource.getStatus());
        } catch (Throwable throwable) {
            log.errorf(throwable, "Error during cleanup for resource %s with name %s: %s",
                    resource.getFullResourceName(),
                    resource.getMetadata().getName(),
                    throwable.getMessage());
            result = DeleteControl.noFinalizerRemoval()
                    .rescheduleAfter(5, TimeUnit.SECONDS);
        }
        return result;
    }

    @Override
    public UpdateControl<T> reconcile(T resource, Context<T> context) throws Exception {
        String lastApplied;
        UpdateControl<T> result;
        final BaseStatus baseStatus = resource.getStatus();
        try {
            result = patchResources(resource, context);
            lastApplied = SerializationUtil.writeAsJson(resource.getSpec());
            baseStatus.setLastApplied(lastApplied);
            log.infof("Reconcilied application %s, reschedule: %s, status: %s",
                    resource.getMetadata().getName(),
                    String.valueOf(result.getScheduleDelay().isPresent()),
                    resource.getStatus());
        } catch (Throwable throwable) {
            log.errorf(throwable, "Error during reconciliation for resource %s with name %s: %s",
                    resource.getFullResourceName(),
                    resource.getMetadata().getName(),
                    throwable.getMessage());
            result = UpdateControl.updateStatus(resource)
                    .rescheduleAfter(5, TimeUnit.SECONDS);
        }
        return result;
    }
}
