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
import com.azure.core.http.rest.Response;
import com.azure.core.util.BinaryData;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AzureBlobCodeStorage implements CodeStorage {

    private static final ObjectMapper mapper = new ObjectMapper();
    protected static final String OBJECT_METADATA_KEY_TENANT = "langstream-tenant";
    protected static final String OBJECT_METADATA_KEY_APPLICATION = "langstream-application";
    protected static final String OBJECT_METADATA_KEY_VERSION = "langstream-version";
    protected static final String OBJECT_METADATA_KEY_PY_BINARIES_DIGEST =
            "langstream-py-binaries-digest";
    protected static final String OBJECT_METADATA_KEY_JAVA_BINARIES_DIGEST =
            "langstream-java-binaries-digest";
    private final String bucketName;
    private final BlobContainerClient blobClient;

    @SneakyThrows
    public AzureBlobCodeStorage(Map<String, Object> configuration) {
        final AzureBlobCodeStorageConfiguration azureConfig =
                mapper.convertValue(configuration, AzureBlobCodeStorageConfiguration.class);

        BlobContainerClientBuilder containerClientBuilder = new BlobContainerClientBuilder();
        containerClientBuilder.endpoint(azureConfig.getEndpoint());
        containerClientBuilder.containerName(azureConfig.getBucketName());

        if (azureConfig.getSasToken() != null) {
            containerClientBuilder.sasToken(azureConfig.getSasToken());
            log.info("Connecting to Azure at {} with SAS token", azureConfig.getEndpoint());
        } else if (azureConfig.getAccountName() != null) {
            containerClientBuilder.credential(new StorageSharedKeyCredential(azureConfig.getAccountName(),
                    azureConfig.getAccountKey()));
            log.info("Connecting to Azure at {} with account name {}", azureConfig.getEndpoint(), azureConfig.getAccountName());
        } else if (azureConfig.getAccountConnectionString() != null) {
            log.info("Connecting to Azure at {} with connection string", azureConfig.getEndpoint());
            containerClientBuilder.credential(StorageSharedKeyCredential.fromConnectionString(
                    azureConfig.getAccountConnectionString()));
        } else {
            throw new IllegalArgumentException("Either sas-token, account-name/account-key or account-connection-string must be provided");
        }

        this.blobClient = containerClientBuilder.buildClient();


        bucketName = azureConfig.getBucketName();

        if (!blobClient.exists()) {
            log.info("Creating bucket {}", bucketName);
            blobClient.createIfNotExists();
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

                final String key = tenant + "/" + codeStoreId;

                final BlobParallelUploadOptions options =
                        new BlobParallelUploadOptions(BinaryData.fromFile(tempFile))
                                .setMetadata(userMetadata);

                final Response<BlockBlobItem> response =
                        blobClient.getBlobClient(key).uploadWithResponse(options, null, Context.NONE);
                if (response.getValue() == null) {
                    throw new CodeStorageException("Failed to upload code archive to azure, status code: " + response.getStatusCode() + ". value was null.");
                }

                return new CodeArchiveMetadata(
                        tenant, codeStoreId, applicationId, pyBinariesDigest, javaBinariesDigest);
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

                final String key = tenant + "/" + codeStoreId;
                blobClient.getBlobClient(key).getBlockBlobClient().downloadToFile(zipFile.toString());
                codeArchive.accept(new LocalZipFileArchiveFile(zipFile));
            } finally {
                if (Files.exists(zipFile)) {
                    Files.delete(zipFile);
                }
                Files.delete(tempFile);
            }
        } catch (IOException e) {
            log.error("Error downloading code archive {} for tenant {}", codeStoreId, tenant, e);
            throw new CodeStorageException(e);
        }
    }

    @Override
    public CodeArchiveMetadata describeApplicationCode(String tenant, String codeStoreId)
            throws CodeStorageException {
        final String key = tenant + "/" + codeStoreId;
        final Boolean exists = blobClient.getBlobClient(key).exists();
        if (exists != null && exists) {
            final Map<String, String> metadata = blobClient.getBlobClient(key).getBlockBlobClient()
                    .getProperties()
                    .getMetadata();
            final String objectTenant = metadata.get(OBJECT_METADATA_KEY_TENANT);
            if (!Objects.equals(objectTenant, tenant)) {
                throw new CodeStorageException(
                        "Tenant mismatch in Azure object "
                        + key
                        + ": "
                        + objectTenant
                        + " != "
                        + tenant);
            }
            final String applicationId = metadata.get(OBJECT_METADATA_KEY_APPLICATION);
            Objects.requireNonNull(
                    applicationId, "Azure object " + key + " contains empty application metadata");

            final String pyBinariesDigest = metadata.get(OBJECT_METADATA_KEY_PY_BINARIES_DIGEST);
            final String javaBinariesDigest =
                    metadata.get(OBJECT_METADATA_KEY_JAVA_BINARIES_DIGEST);

            return new CodeArchiveMetadata(
                    tenant, codeStoreId, applicationId, pyBinariesDigest, javaBinariesDigest);
        } else {
            return null;
        }
    }

    @Override
    public void deleteApplicationCode(String tenant, String codeStoreId)
            throws CodeStorageException {
        blobClient.getBlobClient(codeStoreId).deleteIfExists();
    }

    @Override
    public void deleteApplication(String tenant, String application) throws CodeStorageException {
        // TODO
    }

    @Override
    public void close() {
    }
}
