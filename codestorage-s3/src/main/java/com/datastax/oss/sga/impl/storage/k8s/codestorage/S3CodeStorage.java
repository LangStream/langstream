/**
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
package com.datastax.oss.sga.impl.storage.k8s.codestorage;

import com.datastax.oss.sga.api.codestorage.CodeArchiveMetadata;
import com.datastax.oss.sga.api.codestorage.CodeStorage;
import com.datastax.oss.sga.api.codestorage.CodeStorageException;
import com.datastax.oss.sga.api.codestorage.LocalZipFileArchiveFile;
import com.datastax.oss.sga.api.codestorage.UploadableCodeArchive;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.BucketExistsArgs;
import io.minio.DownloadObjectArgs;
import io.minio.GetObjectArgs;
import io.minio.GetObjectResponse;
import io.minio.MakeBucketArgs;
import io.minio.RemoveObjectArgs;
import io.minio.http.HttpUtils;
import io.minio.messages.Bucket;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import io.minio.MinioClient;
import io.minio.UploadObjectArgs;
import io.minio.errors.MinioException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import okhttp3.OkHttpClient;

@Slf4j
public class S3CodeStorage implements CodeStorage {

    private static final ObjectMapper mapper = new ObjectMapper();
    protected static final long DEFAULT_CONNECTION_TIMEOUT = TimeUnit.MINUTES.toMillis(5L);
    private String bucketName;
    private OkHttpClient httpClient;
    private MinioClient minioClient;

    @SneakyThrows
    public S3CodeStorage(Map<String, Object> configuration) {
        final S3CodeStorageConfiguration s3CodeStorageConfiguration =
                mapper.convertValue(configuration, S3CodeStorageConfiguration.class);

        bucketName = s3CodeStorageConfiguration.getBucketName();
        final String endpoint = s3CodeStorageConfiguration.getEndpoint();
        final String accessKey = s3CodeStorageConfiguration.getAccessKey();
        final String secretKey = s3CodeStorageConfiguration.getSecretKey();

        log.info("Connecting to S3 BlobStorage at {} with accessKey {}", endpoint, accessKey);

        httpClient = HttpUtils.newDefaultHttpClient(DEFAULT_CONNECTION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT,
                DEFAULT_CONNECTION_TIMEOUT);
        minioClient =
                MinioClient.builder()
                        .endpoint(endpoint)
                        .httpClient(httpClient)
                        .credentials(accessKey, secretKey)
                        .build();

        if (!minioClient.bucketExists(BucketExistsArgs.builder()
                .bucket(bucketName)
                .build())) {
            log.info("Creating bucket {}", bucketName);
            minioClient.makeBucket(MakeBucketArgs
                    .builder()
                    .bucket(bucketName)
                    .build());
        } else {
            log.info("Bucket {} already exists", bucketName);
        }
    }


    @Override
    public CodeArchiveMetadata storeApplicationCode(String tenant, String applicationId, String version,
                                                    UploadableCodeArchive codeArchive) throws CodeStorageException {

        try {
            Path tempFile = Files.createTempFile("sga", "upload");
            try {
                Files.copy(codeArchive.getData(), tempFile, StandardCopyOption.REPLACE_EXISTING);
                String codeStoreId = tenant + "_" + applicationId + "_" + version + "_" + UUID.randomUUID();
                log.info("Storing code archive {} for tenant {} and application {} with version {}", codeStoreId,
                        tenant, applicationId, version);

                minioClient.uploadObject(
                        UploadObjectArgs.builder()
                                .userMetadata(Map.of("sga-tenant", tenant,
                                        "sga-application", applicationId,
                                        "sga-version", version))
                                .bucket(bucketName)
                                .object(tenant + "/" + codeStoreId)
                                .contentType("application/zip")
                                .filename(tempFile.toAbsolutePath().toString())
                                .build());
                CodeArchiveMetadata archiveMetadata = new CodeArchiveMetadata(tenant, codeStoreId, applicationId);
                return archiveMetadata;
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
    public void downloadApplicationCode(String tenant, String codeStoreId, DownloadedCodeHandled codeArchive)
            throws CodeStorageException {
        try {
            Path tempFile = Files.createTempDirectory("sga-download-code");
            Path zipFile = tempFile.resolve("code.zip");
            try {
                minioClient.downloadObject(DownloadObjectArgs.builder()
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
    public CodeArchiveMetadata describeApplicationCode(String tenant, String codeStoreId) throws CodeStorageException {
        try {
            // validate that the object exists
            GetObjectResponse object = minioClient.getObject(GetObjectArgs.builder()
                    .bucket(bucketName)
                    .object(tenant + "/" + codeStoreId)
                    .build());
            return new CodeArchiveMetadata(tenant, codeStoreId, null);
        } catch (MinioException | NoSuchAlgorithmException | InvalidKeyException | IOException e) {
            throw new CodeStorageException(e);
        }
    }

    @Override
    public void deleteApplicationCode(String tenant, String codeStoreId) throws CodeStorageException {
        try {
            minioClient.removeObject(RemoveObjectArgs.builder()
                    .bucket(bucketName)
                    .object(codeStoreId)
                    .build());
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
