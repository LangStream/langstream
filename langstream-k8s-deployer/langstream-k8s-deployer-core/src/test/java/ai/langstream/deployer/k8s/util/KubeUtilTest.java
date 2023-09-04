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

import static org.junit.jupiter.api.Assertions.*;

import io.fabric8.kubernetes.api.model.Pod;
import java.util.List;
import org.junit.jupiter.api.Test;

class KubeUtilTest {

    @Test
    public void testOOMKilled() {
        String podYaml =
                """
                apiVersion: v1
                kind: Pod
                metadata:
                  name: pod1
                  namespace: langstream-default
                spec: {subdomain: pod1-s}
                status:
                  containerStatuses:
                  - containerID: docker://7b159d3eca344ef7ed43e31894735fac42e7ee629e0186818074417cf8948165
                    image: xx
                    imageID: xx
                    lastState:
                      terminated:
                        containerID: docker://7b159d3eca344ef7ed43e31894735fac42e7ee629e0186818074417cf8948165
                        exitCode: 137
                        finishedAt: "2023-09-01T17:50:43Z"
                        reason: OOMKilled
                        startedAt: "2023-09-01T17:50:39Z"
                    name: runtime
                    ready: false
                    restartCount: 8
                    started: false
                    state:
                      waiting:
                        message: back-off 5m0s restarting failed container=runtime pod=test-pipeline-python-processor-1-0_langstream-default(a8333379-efc3-406f-a6e4-d2faeadf56bd)
                        reason: CrashLoopBackOff
                  initContainerStatuses:
                  - containerID: docker://efd511fb2a9f17e39dde67fb34e38fc2fba28b973acad74509ae212d804e54f0
                    image: xx
                    imageID: xxxx
                    lastState: {}
                    name: code-download-init
                    ready: true
                    restartCount: 0
                    state:
                      terminated:
                        containerID: docker://efd511fb2a9f17e39dde67fb34e38fc2fba28b973acad74509ae212d804e54f0
                        exitCode: 0
                        finishedAt: "2023-09-01T17:34:23Z"
                        reason: Completed
                        startedAt: "2023-09-01T17:34:23Z"
                  - containerID: docker://cdf904ea4094c4e46142f83f8fe762e569ac28b46d62a11ccd75c647ce019cfa
                    image: xxx
                    imageID: xxx
                    lastState: {}
                    name: code-download
                    ready: true
                    restartCount: 0
                    state:
                      terminated:
                        containerID: docker://cdf904ea4094c4e46142f83f8fe762e569ac28b46d62a11ccd75c647ce019cfa
                        exitCode: 0
                        finishedAt: "2023-09-01T17:34:25Z"
                        reason: Completed
                        startedAt: "2023-09-01T17:34:23Z"
                  phase: Running
                """;

        final Pod pod = SerializationUtil.readYaml(podYaml, Pod.class);
        final KubeUtil.PodStatus status = KubeUtil.getPodsStatuses(List.of(pod)).get("pod1");
        assertEquals(
                "http://pod1.pod1-s.langstream-default.svc.cluster.local:8080", status.getUrl());
        assertEquals(KubeUtil.PodStatus.State.ERROR, status.getState());
        assertEquals("OOMKilled", status.getMessage());
    }

    @Test
    public void testDeployerJobCompleted() {
        String podYaml =
                """
                apiVersion: v1
                kind: Pod
                metadata:
                  name: pod1
                  namespace: langstream-default
                spec: {}
                status:
                  containerStatuses:
                  - containerID: docker://ab5142a1750327f815212b5af5d49b3c6ffa24d10e76b3eacb4b05d781dfda80
                    image: ghcr.io/langstream/langstream-runtime:0.0.8
                    imageID: docker-pullable://ghcr.io/langstream/langstream-runtime@sha256:8e82f1dfb02afe9137adf4a5dff4770705ff5b6607b70206288d164b116ba10d
                    lastState: {}
                    name: deployer
                    ready: false
                    restartCount: 0
                    started: false
                    state:
                      terminated:
                        containerID: docker://ab5142a1750327f815212b5af5d49b3c6ffa24d10e76b3eacb4b05d781dfda80
                        exitCode: 0
                        finishedAt: "2023-09-01T17:32:05Z"
                        reason: Completed
                        startedAt: "2023-09-01T17:32:02Z"
                  initContainerStatuses:
                  - containerID: docker://973604d477c3b301a5ac716eb4d41a4294b92560d8b3adb94f47976b28f0f5f0
                    image: ghcr.io/langstream/langstream-runtime:0.0.8
                    imageID: docker-pullable://ghcr.io/langstream/langstream-runtime@sha256:8e82f1dfb02afe9137adf4a5dff4770705ff5b6607b70206288d164b116ba10d
                    lastState: {}
                    name: deployer-init-config
                    ready: true
                    restartCount: 0
                    state:
                      terminated:
                        containerID: docker://973604d477c3b301a5ac716eb4d41a4294b92560d8b3adb94f47976b28f0f5f0
                        exitCode: 0
                        finishedAt: "2023-09-01T17:32:01Z"
                        reason: Completed
                        startedAt: "2023-09-01T17:32:01Z"
                  phase: Succeeded
                  startTime: "2023-09-01T17:29:19Z"
                """;

        final Pod pod = SerializationUtil.readYaml(podYaml, Pod.class);
        final KubeUtil.PodStatus status = KubeUtil.getPodsStatuses(List.of(pod)).get("pod1");
        assertEquals(KubeUtil.PodStatus.State.COMPLETED, status.getState());
        assertNull(status.getMessage());
    }

    @Test
    public void testDeployerJobRunning() {
        String podYaml =
                """
                        apiVersion: v1
                        kind: Pod
                        metadata:
                          name: pod1
                          namespace: langstream-default
                        spec: {}
                        status:
                          containerStatuses:
                          - containerID: docker://b6f25d48eae76c2a03c9b5b56e020543eaf444c415124cc07b0952c9ae84bc77
                            image: ghcr.io/langstream/langstream-runtime:0.0.8
                            imageID: docker-pullable://ghcr.io/langstream/langstream-runtime@sha256:8e82f1dfb02afe9137adf4a5dff4770705ff5b6607b70206288d164b116ba10d
                            lastState: {}
                            name: deployer
                            ready: true
                            restartCount: 0
                            started: true
                            state:
                              running:
                                startedAt: "2023-09-01T21:09:38Z"
                          hostIP: 192.168.49.2
                          initContainerStatuses:
                          - containerID: docker://a36105bd1d2f17ba799b7c24098ccfb6e64f2026b2b11f02b6446bf64b3e94a5
                            image: ghcr.io/langstream/langstream-runtime:0.0.8
                            imageID: docker-pullable://ghcr.io/langstream/langstream-runtime@sha256:8e82f1dfb02afe9137adf4a5dff4770705ff5b6607b70206288d164b116ba10d
                            lastState: {}
                            name: deployer-init-config
                            ready: true
                            restartCount: 0
                            state:
                              terminated:
                                containerID: docker://a36105bd1d2f17ba799b7c24098ccfb6e64f2026b2b11f02b6446bf64b3e94a5
                                exitCode: 0
                                finishedAt: "2023-09-01T21:09:37Z"
                                reason: Completed
                                startedAt: "2023-09-01T21:09:37Z"
                          phase: Running
                          podIP: 10.244.0.55
                          podIPs:
                          - ip: 10.244.0.55
                          qosClass: Burstable
                          startTime: "2023-09-01T21:09:36Z"
                        """;

        final Pod pod = SerializationUtil.readYaml(podYaml, Pod.class);
        final KubeUtil.PodStatus status = KubeUtil.getPodsStatuses(List.of(pod)).get("pod1");
        assertEquals(KubeUtil.PodStatus.State.RUNNING, status.getState());
        assertNull(status.getMessage());
    }

    @Test
    public void testDeployerJobInitializing() {
        String podYaml =
                """
                        apiVersion: v1
                        kind: Pod
                        metadata:
                          name: pod1
                          namespace: langstream-default
                        spec: {}
                        status:
                          containerStatuses:
                          - containerID: docker://b6f25d48eae76c2a03c9b5b56e020543eaf444c415124cc07b0952c9ae84bc77
                            image: ghcr.io/langstream/langstream-runtime:0.0.8
                            imageID: docker-pullable://ghcr.io/langstream/langstream-runtime@sha256:8e82f1dfb02afe9137adf4a5dff4770705ff5b6607b70206288d164b116ba10d
                            lastState: {}
                            name: deployer
                            ready: true
                            restartCount: 0
                            started: true
                            state:
                              running:
                                startedAt: "2023-09-01T21:09:38Z"
                          hostIP: 192.168.49.2
                          initContainerStatuses:
                          - containerID: docker://a36105bd1d2f17ba799b7c24098ccfb6e64f2026b2b11f02b6446bf64b3e94a5
                            image: ghcr.io/langstream/langstream-runtime:0.0.8
                            imageID: docker-pullable://ghcr.io/langstream/langstream-runtime@sha256:8e82f1dfb02afe9137adf4a5dff4770705ff5b6607b70206288d164b116ba10d
                            lastState: {}
                            name: deployer-init-config
                            ready: true
                            restartCount: 0
                            state:
                              terminated:
                                containerID: docker://a36105bd1d2f17ba799b7c24098ccfb6e64f2026b2b11f02b6446bf64b3e94a5
                                exitCode: 0
                                finishedAt: "2023-09-01T21:09:37Z"
                                reason: Completed
                                startedAt: "2023-09-01T21:09:37Z"
                          phase: Running
                          podIP: 10.244.0.55
                          podIPs:
                          - ip: 10.244.0.55
                          qosClass: Burstable
                          startTime: "2023-09-01T21:09:36Z"
                        """;

        final Pod pod = SerializationUtil.readYaml(podYaml, Pod.class);
        final KubeUtil.PodStatus status = KubeUtil.getPodsStatuses(List.of(pod)).get("pod1");
        assertEquals(KubeUtil.PodStatus.State.RUNNING, status.getState());
        assertNull(status.getMessage());
    }
}
