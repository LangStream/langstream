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
package ai.langstream.deployer.k8s.controllers.apps;

import ai.langstream.api.model.ApplicationLifecycleStatus;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import ai.langstream.deployer.k8s.apps.AppResourcesFactory;
import ai.langstream.deployer.k8s.util.JSONComparator;
import ai.langstream.deployer.k8s.util.KubeUtil;
import ai.langstream.deployer.k8s.util.SpecDiffer;
import ai.langstream.deployer.k8s.controllers.BaseController;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.javaoperatorsdk.operator.api.reconciler.Constants;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusHandler;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;

import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.jbosslog.JBossLog;

@ControllerConfiguration(namespaces = Constants.WATCH_ALL_NAMESPACES, name = "app-controller")
@JBossLog
public class AppController extends BaseController<ApplicationCustomResource> implements
        ErrorStatusHandler<ApplicationCustomResource> {

    @Override
    public ErrorStatusUpdateControl<ApplicationCustomResource> updateErrorStatus(
            ApplicationCustomResource customResource, Context<ApplicationCustomResource> context, Exception e) {
        if (customResource.getStatus() != null
                && customResource.getStatus().getStatus() != null
                && customResource.getStatus().getStatus().getStatus() == ApplicationLifecycleStatus.Status.DELETING) {
            customResource.getStatus().setStatus(ApplicationLifecycleStatus.errorDeleting(e.getMessage()));
        } else {
            customResource.getStatus().setStatus(ApplicationLifecycleStatus.errorDeploying(e.getMessage()));
        }
        return ErrorStatusUpdateControl.updateStatus(customResource);
    }

    @Override
    protected UpdateControl<ApplicationCustomResource> patchResources(ApplicationCustomResource resource,
                                                                      Context<ApplicationCustomResource> context) {
        final boolean reschedule = handleJob(resource, false);
        return reschedule ? UpdateControl.updateStatus(resource)
                .rescheduleAfter(5, TimeUnit.SECONDS) : UpdateControl.updateStatus(resource);
    }

    @Override
    protected DeleteControl cleanupResources(ApplicationCustomResource resource,
                                             Context<ApplicationCustomResource> context) {
        final boolean reschedule = handleJob(resource, true);
        return reschedule ? DeleteControl.noFinalizerRemoval()
                .rescheduleAfter(5, TimeUnit.SECONDS) : DeleteControl.defaultDelete();
    }

    private boolean handleJob(ApplicationCustomResource application, boolean delete) {
        final String jobName = AppResourcesFactory.getJobName(application.getMetadata().getName(), delete);
        final Job currentJob = client.batch().v1().jobs()
                .inNamespace(application.getMetadata().getNamespace())
                .withName(jobName)
                .get();
        if (currentJob == null || areSpecChanged(application)) {
            createJob(application, delete);
            if (!delete) {
                application.getStatus().setStatus(ApplicationLifecycleStatus.DEPLOYING);
            }
            return true;
        } else {
            if (KubeUtil.isJobCompleted(currentJob)) {
                if (!delete) {
                    application.getStatus().setStatus(ApplicationLifecycleStatus.DEPLOYED);
                }
                return false;
            } else {
                return true;
            }
        }
    }

    @SneakyThrows
    private void createJob(ApplicationCustomResource applicationCustomResource, boolean delete) {
        final Job job = AppResourcesFactory.generateJob(applicationCustomResource,
                configuration.getClusterRuntime(), delete, configuration.getPodTemplate());
        KubeUtil.patchJob(client, job);
    }

    protected boolean areSpecChanged(ApplicationCustomResource cr) {
        final String lastApplied = cr.getStatus().getLastApplied();
        if (lastApplied == null) {
            return true;
        }
        final JSONComparator.Result diff = SpecDiffer.generateDiff(lastApplied, cr.getSpec());
        if (!diff.areEquals()) {
            log.infof("Spec changed for %s", cr.getMetadata().getName());
            SpecDiffer.logDetailedSpecDiff(diff);
            return true;
        }
        return false;
    }

}
