package com.datastax.oss.sga.deployer.k8s.controllers.apps;

import com.datastax.oss.sga.api.model.ApplicationLifecycleStatus;
import com.datastax.oss.sga.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import com.datastax.oss.sga.deployer.k8s.apps.AppResourcesFactory;
import com.datastax.oss.sga.deployer.k8s.controllers.BaseController;
import com.datastax.oss.sga.deployer.k8s.util.KubeUtil;
import com.datastax.oss.sga.deployer.k8s.util.SerializationUtil;
import com.datastax.oss.sga.deployer.k8s.util.SpecDiffer;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.javaoperatorsdk.operator.api.reconciler.Constants;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusHandler;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import java.util.Map;
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
        customResource.getStatus().setStatus(ApplicationLifecycleStatus.error(e.getMessage()));
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
        return reschedule ? DeleteControl.defaultDelete()
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
            if (delete) {
                application.getStatus().setStatus(ApplicationLifecycleStatus.DELETING);
            } else {
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

        final Map<String, Object> clusterRuntime;
        if (configuration.clusterRuntime() == null) {
            clusterRuntime = Map.of();
        } else {
            clusterRuntime = SerializationUtil.readYaml(configuration.clusterRuntime(), Map.class);
        }
        final Job job = AppResourcesFactory.generateJob(applicationCustomResource, clusterRuntime, delete);
        KubeUtil.patchJob(client, job);
    }

    protected boolean areSpecChanged(ApplicationCustomResource cr) {
        final String lastApplied = cr.getStatus().getLastApplied();
        if (lastApplied == null) {
            return true;
        }
        return !SpecDiffer.generateDiff(cr.getSpec(), lastApplied).areEquals();
    }

}
