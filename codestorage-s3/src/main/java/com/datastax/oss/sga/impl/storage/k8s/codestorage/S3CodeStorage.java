package com.datastax.oss.sga.impl.storage.k8s.codestorage;

import com.datastax.oss.sga.api.codestorage.CodeArchiveMetadata;
import com.datastax.oss.sga.api.codestorage.CodeStorage;
import com.datastax.oss.sga.api.codestorage.CodeStorageException;
import com.datastax.oss.sga.api.codestorage.DownloadedCodeArchive;
import com.datastax.oss.sga.api.codestorage.LocalZipFileArchiveFile;
import com.datastax.oss.sga.api.codestorage.UploadableCodeArchive;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.DownloadObjectArgs;
import io.minio.GetObjectArgs;
import io.minio.GetObjectResponse;
import io.minio.MakeBucketArgs;
import io.minio.RemoveObjectArgs;
import io.minio.messages.Bucket;
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
import java.util.function.Consumer;

@Slf4j
public class S3CodeStorage implements CodeStorage {

    private final static ObjectMapper MAPPER = new ObjectMapper();

    private String bucketName;
    private MinioClient minioClient;

    @SneakyThrows
    public S3CodeStorage(Map<String, Object> configuration) {
        bucketName = configuration.getOrDefault("bucketName", "sga-code-storage").toString();
        String endpoint = configuration.getOrDefault("endpoint", "http://minio-endpoint.-not-set:9090").toString();
        String username =  configuration.getOrDefault("username", "minioadmin").toString();
        String password =  configuration.getOrDefault("username", "minioadmin").toString();

        log.info("Connecting to S3 BlobStorage at {} with user {}", endpoint, username);

        minioClient =
                MinioClient.builder()
                        .endpoint(endpoint)
                        .credentials(username, password)
                        .build();

        List<Bucket> buckets = minioClient.listBuckets();
        log.info("Existing Buckets: {}", buckets.stream().map(Bucket::name).toList());
        if (!buckets.stream().filter(b->b.name().equals(bucketName)).findAny().isPresent()) {
            log.info("Creating bucket {}", bucketName);
            minioClient.makeBucket(MakeBucketArgs
                    .builder()
                    .bucket(bucketName)
                    .build());
        }
    }



    @Override
    public CodeArchiveMetadata storeApplicationCode(String tenant, String applicationId, String version, UploadableCodeArchive codeArchive) throws CodeStorageException {

        try {
            Path tempFile = Files.createTempFile("sga", "upload");
            try {
                Files.copy(codeArchive.getData(), tempFile, StandardCopyOption.REPLACE_EXISTING);
                String codeStoreId = tenant + "_" + applicationId + "_" + version + "_" + UUID.randomUUID();
                log.info("Storing code archive {} for tenant {} and application {} with version {}", codeStoreId, tenant, applicationId, version);

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
    public void downloadApplicationCode(String tenant, String codeStoreId, DownloadedCodeHandled codeArchive) throws CodeStorageException {
        try {
            Path tempFile = Files.createTempDirectory("sga-download-code");
            Path zipFile = tempFile.resolve("code.zip");
            try {
                minioClient.downloadObject(DownloadObjectArgs.builder()
                                .bucket(bucketName)
                                .filename(zipFile.toAbsolutePath().toString())
                                .object(tenant +"/" + codeStoreId)
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

}
