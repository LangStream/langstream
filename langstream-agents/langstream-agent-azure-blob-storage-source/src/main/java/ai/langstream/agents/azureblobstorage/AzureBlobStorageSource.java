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
package ai.langstream.agents.azureblobstorage;

import ai.langstream.api.runner.code.AbstractAgentCode;
import ai.langstream.api.runner.code.AgentSource;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.util.ConfigurationUtils;
import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.common.StorageSharedKeyCredential;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.AllArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AzureBlobStorageSource extends AbstractAgentCode implements AgentSource {
    private BlobContainerClient client;
    private final Set<String> blobsToCommit = ConcurrentHashMap.newKeySet();
    private int idleTime;

    public static final String ALL_FILES = "*";
    public static final String DEFAULT_EXTENSIONS_FILTER = "pdf,docx,html,htm,md,txt";
    private Set<String> extensions = Set.of();

    static BlobContainerClient createContainerClient(Map<String, Object> configuration) {
        return createContainerClient(
                ConfigurationUtils.getString("container", "langstream-azure-source", configuration),
                ConfigurationUtils.requiredNonEmptyField(
                        configuration, "endpoint", () -> "azure blob storage source"),
                ConfigurationUtils.getString("sas-token", null, configuration),
                ConfigurationUtils.getString("storage-account-name", null, configuration),
                ConfigurationUtils.getString("storage-account-key", null, configuration),
                ConfigurationUtils.getString(
                        "storage-account-connection-string", null, configuration));
    }

    static BlobContainerClient createContainerClient(
            String container,
            String endpoint,
            String sasToken,
            String storageAccountName,
            String storageAccountKey,
            String storageAccountConnectionString) {

        BlobContainerClientBuilder containerClientBuilder = new BlobContainerClientBuilder();
        if (sasToken != null) {
            containerClientBuilder.sasToken(sasToken);
            log.info("Connecting to Azure at {} with SAS token", endpoint);
        } else if (storageAccountName != null) {
            containerClientBuilder.credential(
                    new StorageSharedKeyCredential(storageAccountName, storageAccountKey));
            log.info(
                    "Connecting to Azure at {} with account name {}", endpoint, storageAccountName);
        } else if (storageAccountConnectionString != null) {
            log.info("Connecting to Azure at {} with connection string", endpoint);
            containerClientBuilder.credential(
                    StorageSharedKeyCredential.fromConnectionString(
                            storageAccountConnectionString));
        } else {
            throw new IllegalArgumentException(
                    "Either sas-token, account-name/account-key or account-connection-string must be provided");
        }

        containerClientBuilder.endpoint(endpoint);
        containerClientBuilder.containerName(container);

        final BlobContainerClient containerClient = containerClientBuilder.buildClient();
        log.info(
                "Connected to Azure to account {}, container {}",
                containerClient.getAccountName(),
                containerClient.getBlobContainerName());

        if (!containerClient.exists()) {
            log.info("Creating container");
            containerClient.createIfNotExists();
        } else {
            log.info("Container already exists");
        }
        return containerClient;
    }

    @Override
    public void init(Map<String, Object> configuration) throws Exception {
        client = createContainerClient(configuration);
        idleTime = Integer.parseInt(configuration.getOrDefault("idle-time", 5).toString());
        extensions =
                Set.of(
                        configuration
                                .getOrDefault("file-extensions", DEFAULT_EXTENSIONS_FILTER)
                                .toString()
                                .split(","));

        log.info("Getting files with extensions {} (use '*' to no filter)", extensions);
    }

    @Override
    public List<Record> read() throws Exception {
        List<Record> records = new ArrayList<>();
        final PagedIterable<BlobItem> blobs;
        try {
            blobs = client.listBlobs();
        } catch (Exception e) {
            log.error("Error listing blobs on container {}", client.getBlobContainerName(), e);
            throw e;
        }
        boolean somethingFound = false;
        for (BlobItem blob : blobs) {
            final String name = blob.getName();
            if (blob.isDeleted()) {
                log.debug("Skipping blob {}. deleted status", name);
                continue;
            }
            boolean extensionAllowed = isExtensionAllowed(name, extensions);
            if (!extensionAllowed) {
                log.debug("Skipping blob with bad extension {}", name);
                continue;
            }
            if (!blobsToCommit.contains(name)) {
                log.info("Found new blob {}", name);
                try {
                    byte[] read = client.getBlobClient(name).downloadContent().toBytes();
                    ;
                    records.add(new BlobSourceRecord(read, name));
                    somethingFound = true;
                    blobsToCommit.add(name);
                } catch (Exception e) {
                    log.error("Error reading object {}", name, e);
                    throw e;
                }
                break;
            } else {
                log.info("Skipping already processed object {}", name);
            }
        }
        if (!somethingFound) {
            log.info("Nothing found, sleeping for {} seconds", idleTime);
            Thread.sleep(idleTime * 1000L);
        } else {
            processed(0, 1);
        }
        return records;
    }

    static boolean isExtensionAllowed(String name, Set<String> extensions) {
        if (extensions.contains(ALL_FILES)) {
            return true;
        }
        String extension;
        int extensionIndex = name.lastIndexOf('.');
        if (extensionIndex < 0 || extensionIndex == name.length() - 1) {
            extension = "";
        } else {
            extension = name.substring(extensionIndex + 1);
        }
        return extensions.contains(extension);
    }

    @Override
    protected Map<String, Object> buildAdditionalInfo() {
        return Map.of("container", client.getBlobContainerName());
    }

    @Override
    public void commit(List<Record> records) throws Exception {
        for (Record record : records) {
            BlobSourceRecord blobRecord = (BlobSourceRecord) record;
            String name = blobRecord.name;
            log.info("Removing blob {}", name);
            client.getBlobClient(name).deleteIfExists();
            blobsToCommit.remove(name);
        }
    }

    private static class BlobSourceRecord implements Record {
        private final byte[] read;
        private final String name;
        private final long timestamp = System.currentTimeMillis();

        public BlobSourceRecord(byte[] read, String name) {
            this.read = read;
            this.name = name;
        }

        /**
         * the key is used for routing, so it is better to set it to something meaningful. In case
         * of retransmission the message will be sent to the same partition.
         *
         * @return the key
         */
        @Override
        public Object key() {
            return name;
        }

        @Override
        public Object value() {
            return read;
        }

        @Override
        public String origin() {
            return null;
        }

        @Override
        public Long timestamp() {
            return timestamp;
        }

        @Override
        public Collection<Header> headers() {
            return List.of(new BlobHeader("name", name));
        }

        @AllArgsConstructor
        @ToString
        private static class BlobHeader implements Header {

            final String key;
            final String value;

            @Override
            public String key() {
                return key;
            }

            @Override
            public String value() {
                return value;
            }

            @Override
            public String valueAsString() {
                return value;
            }
        }
    }
}
