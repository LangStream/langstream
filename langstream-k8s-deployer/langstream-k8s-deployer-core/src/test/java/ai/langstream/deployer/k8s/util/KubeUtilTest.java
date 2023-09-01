package ai.langstream.deployer.k8s.util;

import static org.junit.jupiter.api.Assertions.*;
import io.fabric8.kubernetes.api.model.Pod;
import java.util.List;
import org.junit.jupiter.api.Test;

class KubeUtilTest {

    @Test
    public void testOOMKilled() {
        String podYaml = """
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
                        message: back-off 5m0s restarting failed container=runtime pod=test-pipeline-python-function-1-0_langstream-default(a8333379-efc3-406f-a6e4-d2faeadf56bd)
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
        assertEquals("http://pod1.pod1-s.langstream-default.svc.cluster.local:8080", status.getUrl());
        assertEquals(KubeUtil.PodStatus.State.ERROR, status.getState());
        assertEquals("OOMKilled", status.getMessage());
    }

}