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
import ai.langstream.deployer.k8s.api.crds.BaseStatus;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationStatus;
import ai.langstream.deployer.k8s.apps.AppResourcesFactory;
import ai.langstream.deployer.k8s.controllers.BaseController;
import ai.langstream.deployer.k8s.controllers.InfiniteRetry;
import ai.langstream.deployer.k8s.util.JSONComparator;
import ai.langstream.deployer.k8s.util.KubeUtil;
import ai.langstream.deployer.k8s.util.SerializationUtil;
import ai.langstream.deployer.k8s.util.SpecDiffer;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.CustomResource;
import io.javaoperatorsdk.operator.api.reconciler.Constants;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusHandler;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import java.time.Duration;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.jbosslog.JBossLog;

@ControllerConfiguration(
        namespaces = Constants.WATCH_ALL_NAMESPACES,
        name = "app-controller",
        retry = InfiniteRetry.class)
@JBossLog
public class AppController extends BaseController<ApplicationCustomResource>
        implements ErrorStatusHandler<ApplicationCustomResource> {

    private static final ObjectMapper lastAppliedJsonMapper =
            new ObjectMapper()
                    .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

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

    @Data
    @NoArgsConstructor
    public static class AppLastApplied {
        String setup;
        String runtimeDeployer;
    }

    @Override
    protected PatchResult patchResources(
            ApplicationCustomResource resource, Context<ApplicationCustomResource> context) {
        AppLastApplied appLastApplied = getAppLastApplied(resource);
        Duration rescheduleDuration = handleJob(resource, appLastApplied, true, false);
        if (rescheduleDuration == null) {
            log.infof(
                    "setup job for %s is completed, checking deployer",
                    resource.getMetadata().getName());
            rescheduleDuration = handleJob(resource, appLastApplied, false, false);
            log.infof(
                    "setup job for %s is %s",
                    resource.getMetadata().getName(),
                    rescheduleDuration != null ? "not completed" : "completed");
            appLastApplied.setRuntimeDeployer(SerializationUtil.writeAsJson(resource.getSpec()));
        } else {
            if (appLastApplied == null) {
                appLastApplied = new AppLastApplied();
            }
            appLastApplied.setSetup(SerializationUtil.writeAsJson(resource.getSpec()));
            log.infof("setup job for %s is not completed yet", resource.getMetadata().getName());
        }
        final UpdateControl<ApplicationCustomResource> updateControl =
                rescheduleDuration != null
                        ? UpdateControl.updateStatus(resource).rescheduleAfter(rescheduleDuration)
                        : UpdateControl.updateStatus(resource);
        return PatchResult.patch(updateControl).withLastApplied(appLastApplied);
    }

    @Override
    protected DeleteControl cleanupResources(
            ApplicationCustomResource resource, Context<ApplicationCustomResource> context) {
        appResourcesLimiter.onAppBeingDeleted(resource);
        final AppLastApplied appLastApplied = getAppLastApplied(resource);
        Duration rescheduleDuration = handleJob(resource, appLastApplied, false, true);
        if (rescheduleDuration == null) {
            log.infof(
                    "deployer cleanup job for %s is completed, checking setup cleanup",
                    resource.getMetadata().getName());
            rescheduleDuration = handleJob(resource, appLastApplied,true, true);
            log.infof(
                    "setup cleanup job for %s is %s",
                    resource.getMetadata().getName(),
                    rescheduleDuration != null ? "not completed" : "completed");
        } else {
            log.infof(
                    "deployer cleanup job for %s is not completed yet",
                    resource.getMetadata().getName());
        }
        return rescheduleDuration != null
                ? DeleteControl.noFinalizerRemoval().rescheduleAfter(rescheduleDuration)
                : DeleteControl.defaultDelete();
    }

    private Duration handleJob(
            ApplicationCustomResource application, AppLastApplied appLastApplied, boolean isSetupJob, boolean delete) {
        final String applicationId = application.getMetadata().getName();

        final String jobName =
                isSetupJob
                        ? AppResourcesFactory.getSetupJobName(applicationId, delete)
                        : AppResourcesFactory.getDeployerJobName(applicationId, delete);
        final String namespace = application.getMetadata().getNamespace();
        final Job currentJob =
                client.batch().v1().jobs().inNamespace(namespace).withName(jobName).get();
        if (currentJob == null || areSpecChanged(application, appLastApplied, isSetupJob)) {
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



    private static boolean areSpecChanged(ApplicationCustomResource cr, AppLastApplied appLastApplied, boolean checkSetup) {
        if (appLastApplied == null) {
            return true;
        }
        final String lastAppliedAsString = checkSetup ? appLastApplied.getSetup() : appLastApplied.getRuntimeDeployer();
        if (lastAppliedAsString == null) {
            return true;
        }
        final JSONComparator.Result diff = SpecDiffer.generateDiff(lastAppliedAsString, cr.getSpec());
        if (!diff.areEquals()) {
            log.infof("Spec changed for %s", cr.getMetadata().getName());
            SpecDiffer.logDetailedSpecDiff(diff);
            return true;
        }
        return false;
    }

    @SneakyThrows
    private static AppLastApplied getAppLastApplied(ApplicationCustomResource app) {
        if (app.getStatus() == null) {
            return null;
        }
        if (app.getStatus().getLastApplied() == null) {
            return null;
        }
        return lastAppliedJsonMapper.readValue(app.getStatus().getLastApplied(), AppLastApplied.class);

    }
}
