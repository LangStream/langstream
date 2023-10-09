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
import java.io.File;
import java.io.IOException;

public class S3CLIContainerClient implements CodeStorageClient {

    public S3CLIContainerClient(
            String namespace, CodeStorageProvider.CodeStorageConfig codeStorageConfig) {
        this.namespace = namespace;
        this.endpoint = codeStorageConfig.configuration().get("endpoint");
        this.accessKey = codeStorageConfig.configuration().get("access-key");
        this.secretKey = codeStorageConfig.configuration().get("secret-key");
    }

    private final String namespace;
    private String endpoint;
    private String accessKey;
    private String secretKey;
    private boolean started;

    @Override
    public void start() {
        if (!started) {
            deploy();
            started = true;
        }
    }

    private void deploy() {
        BaseEndToEndTest.getClient()
                .resource(
                        new NamespaceBuilder()
                                .withNewMetadata()
                                .withName(namespace)
                                .endMetadata()
                                .build())
                .serverSideApply();
        BaseEndToEndTest.applyManifest(
                """
                        apiVersion: v1
                        kind: Pod
                        metadata:
                          labels:
                            app: s3-client
                          name: s3-client
                        spec:
                          containers:
                          - name: s3-client
                            image: amazon/aws-cli:latest
                            command:
                            - /bin/bash
                            - -c
                            args:
                            - >
                              yum install -y tar &&
                              mkdir -p ~/.aws &&
                              echo -e '[default]' >  ~/.aws/credentials &&
                              echo -e 'aws_access_key_id = %s' >>  ~/.aws/credentials &&
                              echo -e 'aws_secret_access_key = %s' >> ~/.aws/credentials &&
                              echo -e '[default]' >  ~/.aws/config &&
                              echo -e 'region = us-east-1' >>  ~/.aws/config &&
                              echo "Ready" &&
                              sleep infinity
                            resources:
                              requests:
                                cpu: 50m
                                memory: 128Mi
                        """
                        .formatted(accessKey, secretKey),
                namespace);
    }

    @Override
    public void createBucket(String bucketName) throws IOException {
        BaseEndToEndTest.execInPodInNamespace(
                        namespace,
                        "s3-client",
                        "s3-client",
                        """
                aws %s s3 mb s3://%s
                """
                                .formatted(composeEndpointParam(), bucketName))
                .join();
    }

    @Override
    public void deleteBucket(String bucketName) throws IOException {
        BaseEndToEndTest.execInPodInNamespace(
                        namespace,
                        "s3-client",
                        "s3-client",
                        """
                aws %s s3 rm s3://%s
                """
                                .formatted(composeEndpointParam(), bucketName))
                .join();
    }

    private String composeEndpointParam() {
        if (endpoint == null) {
            return "";
        }
        return "--endpoint-url " + endpoint;
    }

    @Override
    public void uploadFromFile(String path, String bucketName, String objectName)
            throws IOException {
        BaseEndToEndTest.copyFileToPod(
                "s3-client", namespace, new File(path), "/tmp/" + objectName);
        BaseEndToEndTest.execInPodInNamespace(
                        namespace,
                        "s3-client",
                        "s3-client",
                        """
                aws %s s3 cp /tmp/%s s3://%s/%s
                """
                                .formatted(
                                        composeEndpointParam(), objectName, bucketName, objectName))
                .join();
    }

    @Override
    public boolean objectExists(String bucketName, String objectName) throws IOException {
        final String result =
                BaseEndToEndTest.execInPodInNamespace(
                                namespace,
                                "s3-client",
                                "s3-client",
                                """
                aws %s s3 ls s3://%s/
                """
                                        .formatted(composeEndpointParam(), bucketName))
                        .join();
        if (result.contains(objectName)) {
            return true;
        }
        return false;
    }
}
