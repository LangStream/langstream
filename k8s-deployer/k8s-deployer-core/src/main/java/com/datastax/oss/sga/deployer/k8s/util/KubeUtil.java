package com.datastax.oss.sga.deployer.k8s.util;

import com.datastax.oss.sga.api.model.ApplicationStatus;
import io.fabric8.kubernetes.api.model.ContainerState;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetStatus;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Getter;
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


    @AllArgsConstructor
    @Getter
    public static class PodStatus {
        public static final PodStatus RUNNING = new PodStatus(State.RUNNING, null);
        public static final PodStatus WAITING = new PodStatus(State.WAITING, null);
        public enum State {
            RUNNING, WAITING, ERROR
        }
        private final State state;
        private final String message;
    }


    public static Map<String, PodStatus> getPodsStatuses(List<Pod> pods) {
        Map<String, PodStatus> podStatuses = new HashMap<>();
        for (Pod pod : pods) {
            PodStatus status = null;

            final List<ContainerStatus> initContainerStatuses = pod.getStatus()
                    .getInitContainerStatuses();
            if (initContainerStatuses != null && !initContainerStatuses.isEmpty()) {
                for (ContainerStatus containerStatus : initContainerStatuses) {
                    status = getStatusFromContainerState(containerStatus.getLastState());
                    if (status == null) {
                        status = getStatusFromContainerState(containerStatus.getState());
                    }
                }
            }
            if (status == null) {
                final List<ContainerStatus> containerStatuses = pod.getStatus()
                        .getContainerStatuses();

                if (containerStatuses.isEmpty()) {
                    status = PodStatus.WAITING;
                } else {
                    // only one container per pod
                    final ContainerStatus containerStatus = containerStatuses.get(0);
                    status = getStatusFromContainerState(containerStatus.getLastState());
                    if (status == null) {
                        status = getStatusFromContainerState(containerStatus.getState());
                    }
                    if (status == null) {
                        status = PodStatus.RUNNING;
                    }
                }
            }
            final String podName = pod.getMetadata().getName();
            podStatuses.put(podName, status);
        }
        return podStatuses;
    }


    private static PodStatus getStatusFromContainerState(ContainerState state) {
        if (state == null) {
            return null;
        }
        if (state.getTerminated() != null) {
            if (state.getTerminated().getMessage() != null) {
                return new PodStatus(PodStatus.State.ERROR, state.getTerminated().getMessage());
            } else {
                return new PodStatus(PodStatus.State.ERROR, "Unknown error");
            }
        } else if (state.getWaiting() != null) {
            return PodStatus.WAITING;
        }
        return null;
    }
}
