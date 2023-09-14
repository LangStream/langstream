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
package ai.langstream.deployer.k8s.controllers.apps;

import ai.langstream.api.model.ApplicationLifecycleStatus;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationStatus;
import ai.langstream.deployer.k8s.apps.AppResourcesFactory;
import ai.langstream.deployer.k8s.controllers.BaseController;
import ai.langstream.deployer.k8s.controllers.InfiniteRetry;
import ai.langstream.deployer.k8s.util.KubeUtil;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.javaoperatorsdk.operator.api.reconciler.Constants;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusHandler;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import java.time.Duration;
import lombok.SneakyThrows;
import lombok.extern.jbosslog.JBossLog;

@ControllerConfiguration(
        namespaces = Constants.WATCH_ALL_NAMESPACES,
        name = "app-controller",
        retry = InfiniteRetry.class)
@JBossLog
public class AppController extends BaseController<ApplicationCustomResource>
        implements ErrorStatusHandler<ApplicationCustomResource> {

    protected static final Duration DEFAULT_RESCHEDULE_DURATION = Duration.ofSeconds(5);

    @Override
    public ErrorStatusUpdateControl<ApplicationCustomResource> updateErrorStatus(
            ApplicationCustomResource customResource,
            Context<ApplicationCustomResource> context,
            Exception e) {
        if (customResource.getStatus() != null
                && customResource.getStatus().getStatus() != null
                && customResource.getStatus().getStatus().getStatus()
                        == ApplicationLifecycleStatus.Status.DELETING) {
            customResource
                    .getStatus()
                    .setStatus(ApplicationLifecycleStatus.errorDeleting(e.getMessage()));
        } else {
            customResource
                    .getStatus()
                    .setStatus(ApplicationLifecycleStatus.errorDeploying(e.getMessage()));
        }
        return ErrorStatusUpdateControl.updateStatus(customResource);
    }

    @Override
    protected UpdateControl<ApplicationCustomResource> patchResources(
            ApplicationCustomResource resource, Context<ApplicationCustomResource> context) {
        Duration rescheduleDuration = handleJob(resource, true, false);
        if (rescheduleDuration == null) {
            rescheduleDuration = handleJob(resource, false, false);
        }
        return rescheduleDuration != null
                ? UpdateControl.updateStatus(resource).rescheduleAfter(rescheduleDuration)
                : UpdateControl.updateStatus(resource);
    }

    @Override
    protected DeleteControl cleanupResources(
            ApplicationCustomResource resource, Context<ApplicationCustomResource> context) {
        appResourcesLimiter.onAppBeingDeleted(resource);
        final Duration rescheduleDuration = handleJob(resource, false, true);
        return rescheduleDuration != null
                ? DeleteControl.noFinalizerRemoval().rescheduleAfter(rescheduleDuration)
                : DeleteControl.defaultDelete();
    }

    private Duration handleJob(
            ApplicationCustomResource application, boolean isSetupJob, boolean delete) {
        final String applicationId = application.getMetadata().getName();

        final String jobName =
                isSetupJob
                        ? AppResourcesFactory.getSetupJobName(applicationId, delete)
                        : AppResourcesFactory.getDeployerJobName(applicationId, delete);
        final String namespace = application.getMetadata().getNamespace();
        final Job currentJob =
                client.batch().v1().jobs().inNamespace(namespace).withName(jobName).get();
        if (currentJob == null || areSpecChanged(application)) {
            if (isSetupJob && !delete) {
                final boolean isDeployable = appResourcesLimiter.checkLimitsForTenant(application);
                if (!isDeployable) {
                    log.infof(
                            "Application %s for tenant %s is not deployable, waiting for resources to be available or limit to be increased.",
                            applicationId, application.getSpec().getTenant());
                    application
                            .getStatus()
                            .setStatus(
                                    ApplicationLifecycleStatus.errorDeploying(
                                            "Not enough resources to deploy application"));
                    application
                            .getStatus()
                            .setResourceLimitStatus(ApplicationStatus.ResourceLimitStatus.REJECTED);
                    return Duration.ofSeconds(30);
                } else {
                    application
                            .getStatus()
                            .setResourceLimitStatus(ApplicationStatus.ResourceLimitStatus.ACCEPTED);
                }
            }
            createJob(application, isSetupJob, delete);
            if (!delete) {
                application.getStatus().setStatus(ApplicationLifecycleStatus.DEPLOYING);
            } else {
                application.getStatus().setStatus(ApplicationLifecycleStatus.DELETING);
            }
            return DEFAULT_RESCHEDULE_DURATION;
        } else {
            if (KubeUtil.isJobCompleted(currentJob)) {
                if (!isSetupJob && !delete) {
                    application.getStatus().setStatus(ApplicationLifecycleStatus.DEPLOYED);
                }
                return null;
            } else {
                return DEFAULT_RESCHEDULE_DURATION;
            }
        }
    }

    @SneakyThrows
    private void createJob(
            ApplicationCustomResource applicationCustomResource, boolean setupJob, boolean delete) {
        final AppResourcesFactory.GenerateJobParams params =
                AppResourcesFactory.GenerateJobParams.builder()
                        .applicationCustomResource(applicationCustomResource)
                        .deleteJob(delete)
                        .clusterRuntimeConfiguration(configuration.getClusterRuntime())
                        .podTemplate(configuration.getAppDeployerPodTemplate())
                        .image(configuration.getRuntimeImage())
                        .imagePullPolicy(configuration.getRuntimeImagePullPolicy())
                        .build();
        final Job job =
                setupJob
                        ? AppResourcesFactory.generateSetupJob(params)
                        : AppResourcesFactory.generateDeployerJob(params);
        KubeUtil.patchJob(client, job);
    }
}
