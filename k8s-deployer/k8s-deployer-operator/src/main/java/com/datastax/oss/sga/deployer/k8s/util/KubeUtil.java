package com.datastax.oss.sga.deployer.k8s.util;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;

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
}
