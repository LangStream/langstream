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
package ai.langstream.impl.storage.k8s.codestorage;

import static org.junit.jupiter.api.Assertions.*;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import ai.langstream.api.codestorage.*;
import io.minio.BucketExistsArgs;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class S3CodeStorageTest {

    private static final DockerImageName localstackImage =
            DockerImageName.parse("localstack/localstack:2.2.0");

    @Container
    private static final LocalStackContainer localstack =
            new LocalStackContainer(localstackImage).withServices(S3);

    @Test
    void testConnection() throws Exception {

        S3CodeStorage storage =
                (S3CodeStorage)
                        new S3CodeStorageProvider()
                                .createImplementation(
                                        "s3",
                                        Map.of(
                                                "type",
                                                "s3",
                                                "bucket-name",
                                                "test-b",
                                                "endpoint",
                                                localstack.getEndpointOverride(S3).toString()));

        CodeArchiveMetadata metadata =
                storage.storeApplicationCode(
                        "mytenant",
                        "myapp",
                        "001",
                        new UploadableCodeArchive() {
                            @Override
                            public InputStream getData() throws IOException, CodeStorageException {
                                return new ByteArrayInputStream("test".getBytes());
                            }

                            @Override
                            public String getPyBinariesDigest() {
                                return null;
                            }

                            @Override
                            public String getJavaBinariesDigest() {
                                return null;
                            }
                        });

        assertEquals("myapp", metadata.applicationId());
        assertEquals("mytenant", metadata.tenant());

        storage.getMinioClient().bucketExists(BucketExistsArgs.builder().bucket("test-b").build());
        storage.downloadApplicationCode(
                metadata.tenant(),
                metadata.codeStoreId(),
                archive -> {
                    try {
                        assertEquals("test", new String(archive.getData()));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
        storage.deleteApplication("mytenant", "myapp-001");

        try {
            storage.downloadApplicationCode(
                    "mytenant",
                    "myapp-001",
                    archive -> {
                        fail("Should not be able to download the archive");
                    });
            fail();
        } catch (CodeStorageException ex) {
            assertTrue(ex.getMessage().contains("Object does not exist"));
        }
    }
}
