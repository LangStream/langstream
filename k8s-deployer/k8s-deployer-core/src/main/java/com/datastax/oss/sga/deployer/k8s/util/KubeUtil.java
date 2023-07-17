package com.datastax.oss.sga.deployer.k8s.util;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetStatus;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KubeUtil {

    private KubeUtil() {
    }

    public static void patchJob(KubernetesClient client, Job resource) {
        final String namespace = resource.getMetadata().getNamespace();
        final Job current = client.resources(resource.getClass())
                .inNamespace(namespace)
                .withName(resource.getMetadata().getName())
                .get();
        if (current != null) {
            client
                    .resource(current)
                    .inNamespace(namespace)
                    .withPropagationPolicy(DeletionPropagation.BACKGROUND)
                    .delete();
        }
        client.resource(resource)
                .inNamespace(namespace)
                .create();
    }



    public static boolean isJobCompleted(Job job) {
        if (job == null) {
            return false;
        }
        final Integer succeeded = job.getStatus().getSucceeded();
        return succeeded != null && succeeded > 0;
    }

    public static void patchStatefulSet(KubernetesClient client, StatefulSet resource) {
        final String namespace = resource.getMetadata().getNamespace();
        client.resource(resource)
                .inNamespace(namespace)
                .serverSideApply();
    }


    public static boolean isStatefulSetReady(StatefulSet sts) {
        if (sts == null || sts.getStatus() == null) {
            return false;
        }
        final StatefulSetStatus status = sts.getStatus();
        if (!Objects.equals(status.getCurrentRevision(), status.getUpdateRevision())) {
            log.debug("statefulset {} is not ready, revision mismatch {} - {}", sts.getMetadata().getName(),
                    status.getCurrentRevision(), status.getUpdateRevision());
            return false;
        }
        if (log.isDebugEnabled()) {
            log.debug("check if statefulset {} is ready, replicas {}, ready replicas {}, updated replicas {}",
                    sts.getMetadata().getName(),
                    status.getReplicas(), status.getReadyReplicas(), status.getUpdatedReplicas());
        }
        if (status.getReplicas() == null || status.getReadyReplicas() == null || status.getUpdatedReplicas() == null) {
            return false;
        }

        final int replicas = status.getReplicas().intValue();
        final int ready = status.getReadyReplicas().intValue();
        final int updated = status.getUpdatedReplicas().intValue();
        return replicas == ready && updated == ready;
    }
}
