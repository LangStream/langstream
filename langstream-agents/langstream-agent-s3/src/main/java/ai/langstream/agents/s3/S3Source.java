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
package ai.langstream.agents.s3;

import ai.langstream.api.runner.code.AbstractAgentCode;
import ai.langstream.api.runner.code.AgentSource;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.Record;
import io.minio.BucketExistsArgs;
import io.minio.GetObjectArgs;
import io.minio.GetObjectResponse;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.RemoveObjectArgs;
import io.minio.Result;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import io.minio.messages.Item;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
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
public class S3Source extends AbstractAgentCode implements AgentSource {
    private String bucketName;
    private MinioClient minioClient;
    private final Set<String> objectsToCommit = ConcurrentHashMap.newKeySet();
    private int idleTime;

    public static final String ALL_FILES = "*";
    public static final String DEFAULT_EXTENSIONS_FILTER = "pdf,docx,html,htm,md,txt";
    private Set<String> extensions = Set.of();

    @Override
    public void init(Map<String, Object> configuration) throws Exception {
        bucketName = configuration.getOrDefault("bucketName", "langstream-source").toString();
        String endpoint =
                configuration
                        .getOrDefault("endpoint", "http://minio-endpoint.-not-set:9090")
                        .toString();
        String username = configuration.getOrDefault("access-key", "minioadmin").toString();
        String password = configuration.getOrDefault("secret-key", "minioadmin").toString();
        String region = configuration.getOrDefault("region", "").toString();
        idleTime = Integer.parseInt(configuration.getOrDefault("idle-time", 5).toString());
        extensions =
                Set.of(
                        configuration
                                .getOrDefault("file-extensions", DEFAULT_EXTENSIONS_FILTER)
                                .toString()
                                .split(","));

        log.info(
                "Connecting to S3 Bucket at {} in region {} with user {}",
                endpoint,
                region,
                username);
        log.info("Getting files with extensions {} (use '*' to no filter)", extensions);

        MinioClient.Builder builder =
                MinioClient.builder().endpoint(endpoint).credentials(username, password);
        if (!region.isBlank()) {
            builder.region(region);
        }
        minioClient = builder.build();

        makeBucketIfNotExists(bucketName);
    }

    private void makeBucketIfNotExists(String bucketName)
            throws ServerException,
                    InsufficientDataException,
                    ErrorResponseException,
                    IOException,
                    NoSuchAlgorithmException,
                    InvalidKeyException,
                    InvalidResponseException,
                    XmlParserException,
                    InternalException {
        if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build())) {
            log.info("Creating bucket {}", bucketName);
            minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
        } else {
            log.info("Bucket {} already exists", bucketName);
        }
    }

    @Override
    public List<Record> read() throws Exception {
        List<Record> records = new ArrayList<>();
        Iterable<Result<Item>> results;
        try {
            results = minioClient.listObjects(ListObjectsArgs.builder().bucket(bucketName).build());
        } catch (Exception e) {
            log.error("Error listing objects on bucket {}", bucketName, e);
            throw e;
        }
        boolean somethingFound = false;
        for (Result<Item> object : results) {
            Item item = object.get();
            String name = item.objectName();
            if (item.isDir()) {
                log.debug("Skipping directory {}", name);
                continue;
            }
            boolean extensionAllowed = isExtensionAllowed(name, extensions);
            if (!extensionAllowed) {
                log.debug("Skipping file with bad extension {}", name);
                continue;
            }
            if (!objectsToCommit.contains(name)) {
                log.info("Found new object {}, size {} KB", item.objectName(), item.size() / 1024);
                try {
                    GetObjectResponse objectResponse =
                            minioClient.getObject(
                                    GetObjectArgs.builder()
                                            .bucket(bucketName)
                                            .object(name)
                                            .build());
                    objectsToCommit.add(name);
                    byte[] read = objectResponse.readAllBytes();
                    records.add(new S3SourceRecord(read, name));
                    somethingFound = true;
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
        return Map.of("bucketName", bucketName);
    }

    @Override
    public void commit(List<Record> records) throws Exception {
        for (Record record : records) {
            S3SourceRecord s3SourceRecord = (S3SourceRecord) record;
            String objectName = s3SourceRecord.name;
            log.info("Removing object {}", objectName);
            minioClient.removeObject(
                    RemoveObjectArgs.builder().bucket(bucketName).object(objectName).build());
            objectsToCommit.remove(objectName);
        }
    }

    private static class S3SourceRecord implements Record {
        private final byte[] read;
        private final String name;

        public S3SourceRecord(byte[] read, String name) {
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
            return System.currentTimeMillis();
        }

        @Override
        public Collection<Header> headers() {
            return List.of(new S3RecordHeader("name", name));
        }

        @AllArgsConstructor
        @ToString
        private static class S3RecordHeader implements Header {

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
