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

public class AzureBlobCLIContainerClient implements CodeStorageClient {

    public AzureBlobCLIContainerClient(
            String namespace, CodeStorageProvider.CodeStorageConfig codeStorageConfig) {
        this.namespace = namespace;
        this.accountName = codeStorageConfig.configuration().get("storage-account-name");
        this.accountKey = codeStorageConfig.configuration().get("storage-account-key");
    }

    private final String namespace;
    private String accountName;
    private String accountKey;
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
                            app: azure-blob-client
                          name: azure-blob-client
                        spec:
                          containers:
                          - name: azure-blob-client
                            image: mcr.microsoft.com/azure-cli:latest
                            command:
                            - /bin/bash
                            - -c
                            args:
                            - >
                              echo "Ready" &&
                              sleep infinity
                            resources:
                              requests:
                                cpu: 50m
                                memory: 128Mi
                        """,
                namespace);
    }

    @Override
    public void createBucket(String bucketName) throws IOException {
        BaseEndToEndTest.execInPodInNamespace(
                        namespace,
                        "azure-blob-client",
                        "azure-blob-client",
                        """
                az storage container create -n %s --fail-on-exist %s
                """
                                .formatted(bucketName, composeAccountParam()))
                .join();
    }

    @Override
    public void deleteBucket(String bucketName) throws IOException {
        BaseEndToEndTest.execInPodInNamespace(
                        namespace,
                        "azure-blob-client",
                        "azure-blob-client",
                        """
                    az storage container delete -n %s %s
                    """
                                .formatted(bucketName, composeAccountParam()))
                .join();
    }

    @Override
    public void uploadFromFile(String path, String bucketName, String objectName)
            throws IOException {
        BaseEndToEndTest.copyFileToPod(
                "azure-blob-client", namespace, new File(path), "/tmp/" + objectName);
        BaseEndToEndTest.execInPodInNamespace(
                        namespace,
                        "azure-blob-client",
                        "azure-blob-client",
                        """
                az storage blob upload -f /tmp/%s -c %s -n %s %s
                """
                                .formatted(
                                        objectName, bucketName, objectName, composeAccountParam()))
                .join();
    }

    @Override
    public boolean objectExists(String bucketName, String objectName) throws IOException {
        final String result =
                BaseEndToEndTest.execInPodInNamespace(
                                namespace,
                                "azure-blob-client",
                                "azure-blob-client",
                                """
                az storage blob exists -c %s -n %s %s
                """
                                        .formatted(bucketName, objectName, composeAccountParam()))
                        .join();
        if (result.contains(objectName)) {
            return true;
        }
        return false;
    }

    private String composeAccountParam() {
        return " --account-name %s --account-key %s ".formatted(accountName, accountKey);
    }
}
