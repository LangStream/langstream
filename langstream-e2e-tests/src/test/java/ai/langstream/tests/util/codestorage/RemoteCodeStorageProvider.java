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

import ai.langstream.tests.util.CodeStorageProvider;
import ai.langstream.tests.util.SystemOrEnv;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RemoteCodeStorageProvider implements CodeStorageProvider {

    private static final String SYS_PROPERTIES_PREFIX = "langstream.tests.codestorageremote.props.";
    private static final String ENV_PREFIX = "LANGSTREAM_TESTS_CODESTORAGEREMOTE_PROPS_";
    private static final String TYPE;
    private static final Map<String, String> CONFIG;
    private Set<String> createdBuckets = new HashSet<>();
    private CodeStorageClient codeStorageClient;

    static {
        CONFIG = new HashMap<>();
        final Map<String, String> props =
                SystemOrEnv.getProperties(ENV_PREFIX, SYS_PROPERTIES_PREFIX);
        props.forEach(
                (key, value) -> {
                    final String newKey = key.replace("_", "-");
                    CONFIG.put(newKey, value);
                    log.info("Loading remote codestorage config: {}={}", newKey, value);
                });
        TYPE =
                SystemOrEnv.getProperty(
                        "LANGSTREAM_TESTS_CODESTORAGEREMOTE_TYPE",
                        "langstream.tests.codestorageremote.type");
    }

    @Override
    public CodeStorageConfig start() {
        if (TYPE == null) {
            throw new IllegalArgumentException(
                    "langstream.tests.codestorageremote.type must be set");
        }
        final CodeStorageConfig codeStorageConfig = new CodeStorageConfig(TYPE, CONFIG);
        if (codeStorageClient == null) {
            if (TYPE.equals("s3")) {
                codeStorageClient =
                        new S3CLIContainerClient("ls-test-s3-client", codeStorageConfig);
                codeStorageClient.start();
            } else if (TYPE.equals("azure")) {
                codeStorageClient =
                        new AzureBlobCLIContainerClient("ls-test-azure-client", codeStorageConfig);
                codeStorageClient.start();
            }
        }
        return codeStorageConfig;
    }

    @Override
    public void cleanup() {
        for (String b : createdBuckets) {
            try {
                codeStorageClient.deleteBucket(b);
            } catch (IOException e) {
                log.error("Failed to delete bucket {}", b, e);
            }
        }
        createdBuckets.clear();
    }

    @Override
    public void stop() {}

    @Override
    public void createBucket(String bucketName) throws IOException {
        checkClient();
        codeStorageClient.createBucket(bucketName);
        createdBuckets.add(bucketName);
    }

    private void checkClient() {
        if (codeStorageClient == null) {
            throw new UnsupportedOperationException(
                    "code storage client not supported for type " + TYPE);
        }
    }

    @Override
    public void uploadFromFile(String path, String bucketName, String objectName)
            throws IOException {
        checkClient();
        codeStorageClient.uploadFromFile(path, bucketName, objectName);
    }

    @Override
    public boolean objectExists(String bucketName, String objectName) throws IOException {
        checkClient();
        return codeStorageClient.objectExists(bucketName, objectName);
    }
}
