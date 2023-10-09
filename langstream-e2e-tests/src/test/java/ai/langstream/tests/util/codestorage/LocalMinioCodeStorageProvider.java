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
package ai.langstream.tests.util.codestorage;

import ai.langstream.tests.util.BaseEndToEndTest;
import ai.langstream.tests.util.CodeStorageProvider;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import java.io.IOException;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LocalMinioCodeStorageProvider implements CodeStorageProvider {

    protected static final String NAMESPACE = "ls-test-minio";
    private boolean started;
    private CodeStorageClient codeStorageClient;

    @Override
    public CodeStorageConfig start() {
        final CodeStorageConfig config = createCodeConfig();
        if (!started) {
            deploy();
            codeStorageClient = new S3CLIContainerClient(NAMESPACE, config);
            codeStorageClient.start();
            started = true;
        }
        return config;
    }

    private CodeStorageConfig createCodeConfig() {
        return new CodeStorageConfig(
                "s3",
                Map.of(
                        "endpoint", getEndpoint(),
                        "access-key", "minioadmin",
                        "secret-key", "minioadmin"));
    }

    private String getEndpoint() {
        return "http://minio.%s.svc.cluster.local:9000".formatted(NAMESPACE);
    }

    private void deploy() {
        BaseEndToEndTest.getClient()
                .resource(
                        new NamespaceBuilder()
                                .withNewMetadata()
                                .withName(NAMESPACE)
                                .endMetadata()
                                .build())
                .serverSideApply();
        log.info("Deploying MinIO");
        BaseEndToEndTest.applyManifest(
                """
                        # Deploys a new MinIO Pod into the metadata.namespace Kubernetes namespace
                        #
                        # The `spec.containers[0].args` contains the command run on the pod
                        # The `/data` directory corresponds to the `spec.containers[0].volumeMounts[0].mountPath`
                        # That mount path corresponds to a Kubernetes HostPath which binds `/data` to a local drive or volume on the worker node where the pod runs
                        #\s
                        apiVersion: v1
                        kind: Pod
                        metadata:
                          labels:
                            app: minio
                          name: minio
                        spec:
                          containers:
                          - name: minio
                            image: quay.io/minio/minio:latest
                            command:
                            - /bin/bash
                            - -c
                            args:\s
                            - minio server /data --console-address :9090
                            volumeMounts:
                            - mountPath: /data
                              name: localvolume # Corresponds to the `spec.volumes` Persistent Volume
                            ports:
                              -  containerPort: 9090
                                 protocol: TCP
                                 name: console
                              -  containerPort: 9000
                                 protocol: TCP
                                 name: s3
                            resources:
                              requests:
                                cpu: 50m
                                memory: 512Mi
                          volumes:
                          - name: localvolume
                            hostPath: # MinIO generally recommends using locally-attached volumes
                              path: /mnt/disk1/data # Specify a path to a local drive or volume on the Kubernetes worker node
                              type: DirectoryOrCreate # The path to the last directory must exist
                        ---
                        apiVersion: v1
                        kind: Service
                        metadata:
                          labels:
                            app: minio
                          name: minio
                        spec:
                          ports:
                            - port: 9090
                              protocol: TCP
                              targetPort: 9090
                              name: console
                            - port: 9000
                              protocol: TCP
                              targetPort: 9000
                              name: s3
                          selector:
                            app: minio
                        """,
                NAMESPACE);
        log.info("MinIO ready");
    }

    @Override
    public void cleanup() {}

    @Override
    public void stop() {
        BaseEndToEndTest.getClient().namespaces().withName(NAMESPACE).delete();
    }

    @Override
    public void createBucket(String bucketName) throws IOException {
        codeStorageClient.createBucket(bucketName);
    }

    @Override
    public void uploadFromFile(String path, String bucketName, String objectName)
            throws IOException {
        codeStorageClient.uploadFromFile(path, bucketName, objectName);
    }

    @Override
    @SneakyThrows
    public boolean objectExists(String bucketName, String objectName) throws IOException {
        return codeStorageClient.objectExists(bucketName, objectName);
    }
}
