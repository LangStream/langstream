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

import ai.langstream.api.codestorage.CodeArchiveMetadata;
import ai.langstream.api.codestorage.CodeStorage;
import ai.langstream.api.codestorage.CodeStorageException;
import ai.langstream.api.codestorage.LocalZipFileArchiveFile;
import ai.langstream.api.codestorage.UploadableCodeArchive;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.BucketExistsArgs;
import io.minio.DownloadObjectArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.RemoveObjectArgs;
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;
import io.minio.UploadObjectArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.MinioException;
import io.minio.http.HttpUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;

@Slf4j
public class S3CodeStorage implements CodeStorage {

    private static final ObjectMapper mapper = new ObjectMapper();
    protected static final long DEFAULT_CONNECTION_TIMEOUT = TimeUnit.MINUTES.toMillis(5L);
    protected static final String OBJECT_METADATA_KEY_TENANT = "langstream-tenant";
    protected static final String OBJECT_METADATA_KEY_APPLICATION = "langstream-application";
    protected static final String OBJECT_METADATA_KEY_VERSION = "langstream-version";
    protected static final String OBJECT_METADATA_KEY_PY_BINARIES_DIGEST =
            "langstream-py-binaries-digest";
    protected static final String OBJECT_METADATA_KEY_JAVA_BINARIES_DIGEST =
            "langstream-java-binaries-digest";
    private final String bucketName;
    private final OkHttpClient httpClient;
    private final MinioClient minioClient;

    @SneakyThrows
    public S3CodeStorage(Map<String, Object> configuration) {
        final S3CodeStorageConfiguration s3CodeStorageConfiguration =
                mapper.convertValue(configuration, S3CodeStorageConfiguration.class);

        bucketName = s3CodeStorageConfiguration.getBucketName();
        final String endpoint = s3CodeStorageConfiguration.getEndpoint();
        final String accessKey = s3CodeStorageConfiguration.getAccessKey();
        final String secretKey = s3CodeStorageConfiguration.getSecretKey();

        log.info("Connecting to S3 BlobStorage at {} with accessKey {}", endpoint, accessKey);

        httpClient =
                HttpUtils.newDefaultHttpClient(
                        DEFAULT_CONNECTION_TIMEOUT,
                        DEFAULT_CONNECTION_TIMEOUT,
                        DEFAULT_CONNECTION_TIMEOUT);
        minioClient =
                MinioClient.builder()
                        .endpoint(endpoint)
                        .httpClient(httpClient)
                        .credentials(accessKey, secretKey)
                        .build();

        if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build())) {
            log.info("Creating bucket {}", bucketName);
            minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
        } else {
            log.info("Bucket {} already exists", bucketName);
        }
    }

    @Override
    public CodeArchiveMetadata storeApplicationCode(
            String tenant, String applicationId, String version, UploadableCodeArchive codeArchive)
            throws CodeStorageException {

        try {
            Path tempFile = Files.createTempFile("langstream", "upload");
            try {
                Files.copy(codeArchive.getData(), tempFile, StandardCopyOption.REPLACE_EXISTING);
                String codeStoreId =
                        tenant + "_" + applicationId + "_" + version + "_" + UUID.randomUUID();
                log.info(
                        "Storing code archive {} for tenant {} and application {} with version {}",
                        codeStoreId,
                        tenant,
                        applicationId,
                        version);

                final String javaBinariesDigest = codeArchive.getJavaBinariesDigest();
                final String pyBinariesDigest = codeArchive.getPyBinariesDigest();

                final Map<String, String> userMetadata = new HashMap<>();
                userMetadata.put(OBJECT_METADATA_KEY_TENANT, tenant);
                userMetadata.put(OBJECT_METADATA_KEY_APPLICATION, applicationId);
                userMetadata.put(OBJECT_METADATA_KEY_VERSION, version);
                if (javaBinariesDigest != null) {
                    userMetadata.put(OBJECT_METADATA_KEY_JAVA_BINARIES_DIGEST, javaBinariesDigest);
                }
                if (pyBinariesDigest != null) {
                    userMetadata.put(OBJECT_METADATA_KEY_PY_BINARIES_DIGEST, pyBinariesDigest);
                }
                minioClient.uploadObject(
                        UploadObjectArgs.builder()
                                .userMetadata(userMetadata)
                                .bucket(bucketName)
                                .object(tenant + "/" + codeStoreId)
                                .contentType("application/zip")
                                .filename(tempFile.toAbsolutePath().toString())
                                .build());
                return new CodeArchiveMetadata(
                        tenant, codeStoreId, applicationId, pyBinariesDigest, javaBinariesDigest);
            } catch (MinioException | NoSuchAlgorithmException | InvalidKeyException e) {
                throw new CodeStorageException(e);
            } finally {
                Files.delete(tempFile);
            }
        } catch (IOException err) {
            throw new CodeStorageException(err);
        }
    }

    @Override
    public void downloadApplicationCode(
            String tenant, String codeStoreId, DownloadedCodeHandled codeArchive)
            throws CodeStorageException {
        try {
            Path tempFile = Files.createTempDirectory("langstream-download-code");
            Path zipFile = tempFile.resolve("code.zip");
            try {
                minioClient.downloadObject(
                        DownloadObjectArgs.builder()
                                .bucket(bucketName)
                                .filename(zipFile.toAbsolutePath().toString())
                                .object(tenant + "/" + codeStoreId)
                                .build());
                codeArchive.accept(new LocalZipFileArchiveFile(zipFile));
            } finally {
                if (Files.exists(zipFile)) {
                    Files.delete(zipFile);
                }
                Files.delete(tempFile);
            }
        } catch (MinioException | NoSuchAlgorithmException | InvalidKeyException | IOException e) {
            log.error("Error downloading code archive {} for tenant {}", codeStoreId, tenant, e);
            throw new CodeStorageException(e);
        }
    }

    @Override
    public CodeArchiveMetadata describeApplicationCode(String tenant, String codeStoreId)
            throws CodeStorageException {
        try {
            final String objectName = tenant + "/" + codeStoreId;
            final StatObjectResponse statObjectResponse =
                    minioClient.statObject(
                            StatObjectArgs.builder().bucket(bucketName).object(objectName).build());
            final Map<String, String> metadata = statObjectResponse.userMetadata();
            final String objectTenant = metadata.get(OBJECT_METADATA_KEY_TENANT);
            if (!Objects.equals(objectTenant, tenant)) {
                throw new CodeStorageException(
                        "Tenant mismatch in S3 object "
                                + objectName
                                + ": "
                                + objectTenant
                                + " != "
                                + tenant);
            }
            final String applicationId = metadata.get(OBJECT_METADATA_KEY_APPLICATION);
            Objects.requireNonNull(
                    applicationId, "S3 object " + objectName + " contains empty application");

            final String pyBinariesDigest = metadata.get(OBJECT_METADATA_KEY_PY_BINARIES_DIGEST);
            final String javaBinariesDigest =
                    metadata.get(OBJECT_METADATA_KEY_JAVA_BINARIES_DIGEST);

            return new CodeArchiveMetadata(
                    tenant, codeStoreId, applicationId, pyBinariesDigest, javaBinariesDigest);
        } catch (ErrorResponseException errorResponseException) {
            // https://github.com/minio/minio-java/blob/7ca9500165ee13d39f293691943b93c19c31ebc2/api/src/main/java/io/minio/S3Base.java#L682-L692
            if ("NoSuchKey".equals(errorResponseException.errorResponse().code())) {
                return null;
            }
            throw new CodeStorageException(errorResponseException);
        } catch (MinioException | NoSuchAlgorithmException | InvalidKeyException | IOException e) {
            throw new CodeStorageException(e);
        }
    }

    @Override
    public void deleteApplicationCode(String tenant, String codeStoreId)
            throws CodeStorageException {
        try {
            minioClient.removeObject(
                    RemoveObjectArgs.builder().bucket(bucketName).object(codeStoreId).build());
        } catch (MinioException | NoSuchAlgorithmException | InvalidKeyException | IOException e) {
            throw new CodeStorageException(e);
        }
    }

    @Override
    public void deleteApplication(String tenant, String application) throws CodeStorageException {
        // TODO
    }

    @Override
    public void close() {
        if (httpClient != null) {
            httpClient.dispatcher().executorService().shutdown();
            httpClient.connectionPool().evictAll();
            if (httpClient.cache() != null) {
                try {
                    httpClient.cache().close();
                } catch (IOException e) {
                    log.error("Error closing okhttpclient", e);
                }
            }
        }
    }
}
