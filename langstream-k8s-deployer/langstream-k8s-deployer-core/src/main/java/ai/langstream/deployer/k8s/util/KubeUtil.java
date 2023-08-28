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
package ai.langstream.deployer.k8s.util;

import io.fabric8.kubernetes.api.model.ContainerState;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
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

        final int replicas = status.getReplicas();
        final int ready = status.getReadyReplicas();
        final int updated = status.getUpdatedReplicas();
        return replicas == ready && updated == ready;
    }


    @AllArgsConstructor
    @Getter
    public static class PodStatus {
        public static final PodStatus RUNNING = new PodStatus(State.RUNNING, null, null);
        public static final PodStatus WAITING = new PodStatus(State.WAITING, null, null);
        public enum State {
            RUNNING, WAITING, ERROR
        }
        private final State state;
        private final String message;
        private final String url;

        public PodStatus withUrl(String url) {
            return new PodStatus(state, message, url);
        }
    }


    public static Map<String, PodStatus> getPodsStatuses(List<Pod> pods) {
        Map<String, PodStatus> podStatuses = new HashMap<>();
        for (Pod pod : pods) {
            log.info("pod name={} namespace={} status {}",
                    pod.getMetadata().getName(),
                    pod.getMetadata().getNamespace(),
                    pod.getStatus());
            PodStatus status = null;

            final List<ContainerStatus> initContainerStatuses = pod.getStatus()
                    .getInitContainerStatuses();
            if (initContainerStatuses != null && !initContainerStatuses.isEmpty()) {
                for (ContainerStatus containerStatus : initContainerStatuses) {
                    status = getStatusFromContainerState(containerStatus.getLastState(), true);
                    if (status == null) {
                        status = getStatusFromContainerState(containerStatus.getState(), true);
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
                    status = getStatusFromContainerState(containerStatus.getLastState(), false);
                    if (status == null) {
                        status = getStatusFromContainerState(containerStatus.getState(), false);
                    }
                    if (status == null) {
                        status = PodStatus.RUNNING;
                    }
                }
            }
            final String podName = pod.getMetadata().getName();
            // this is podname + servicename + namespace
            String podUrl = "http://" + pod.getMetadata().getName() + "." +
                    pod.getSpec().getSubdomain() + "." +
                    pod.getMetadata().getNamespace() + ".svc.cluster.local:8080";
            log.info("Pod url: {}", podUrl);
            status = status.withUrl(podUrl);

            podStatuses.put(podName, status);
        }
        return podStatuses;
    }


    private static PodStatus getStatusFromContainerState(ContainerState state, boolean isInit) {
        if (state == null) {
            return null;
        }
        if (state.getTerminated() != null) {
            if (isInit && state.getTerminated().getReason().equals("Completed")) {
                return null;
            }
            if (state.getTerminated().getMessage() != null) {
                return new PodStatus(PodStatus.State.ERROR, state.getTerminated().getMessage(),  null);
            } else {
                return new PodStatus(PodStatus.State.ERROR, "Unknown error", null);
            }
        } else if (state.getWaiting() != null) {
            return PodStatus.WAITING;
        }
        return null;
    }
    public static OwnerReference getOwnerReferenceForResource(HasMetadata resource) {
        return new OwnerReferenceBuilder()
                .withApiVersion(resource.getApiVersion())
                .withKind(resource.getKind())
                .withName(resource.getMetadata().getName())
                .withUid(resource.getMetadata().getUid())
                .withBlockOwnerDeletion(true)
                .withController(true)
                .build();
    }
}
