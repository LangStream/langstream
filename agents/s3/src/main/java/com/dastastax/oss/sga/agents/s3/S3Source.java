package com.dastastax.oss.sga.agents.s3;

import com.datastax.oss.sga.api.runner.code.AgentSource;
import com.datastax.oss.sga.api.runner.code.Header;
import com.datastax.oss.sga.api.runner.code.Record;
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
import io.minio.messages.Bucket;
import io.minio.messages.Item;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import lombok.AllArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class S3Source implements AgentSource {
    private String bucketName;
    private MinioClient minioClient;
    private final Set<String> objectsToCommit = ConcurrentHashMap.newKeySet();
    private int idleTime;

    @Override
    public void init(Map<String, Object> configuration) throws Exception {
        bucketName = configuration.getOrDefault("bucketName", "sga-source").toString();
        String endpoint = configuration.getOrDefault("endpoint", "http://minio-endpoint.-not-set:9090").toString();
        String username =  configuration.getOrDefault("username", "minioadmin").toString();
        String password =  configuration.getOrDefault("password", "minioadmin").toString();
        idleTime = Integer.parseInt(configuration.getOrDefault("idle-time", 5).toString());

        log.info("Connecting to S3 BlobStorage at {} with user {}", endpoint, username);

        minioClient =
            MinioClient.builder()
                .endpoint(endpoint)
                .credentials(username, password)
                .build();

        List<Bucket> buckets = minioClient.listBuckets();
        log.info("Existing Buckets: {}", buckets.stream().map(Bucket::name).toList());
        makeBucketIfNotExists(buckets, bucketName);
    }

    private void makeBucketIfNotExists(List<Bucket> buckets, String bucketName)
        throws ServerException, InsufficientDataException, ErrorResponseException, IOException,
        NoSuchAlgorithmException, InvalidKeyException, InvalidResponseException, XmlParserException, InternalException {
        if (buckets.stream().noneMatch(b->b.name().equals(bucketName))) {
            log.info("Creating bucket {}", bucketName);
            minioClient.makeBucket(MakeBucketArgs
                .builder()
                .bucket(bucketName)
                .build());
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
                log.info("Skipping directory {}", name);
                continue;
            }
            if (!objectsToCommit.contains(name)) {
                log.info("Found new object {}, size {} KB", item.objectName(), item.size() / 1024);
                try {
                    GetObjectResponse objectResponse = minioClient.getObject(
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
            Thread.sleep(idleTime * 1000);
        }
        return records;
    }

    @Override
    public void commit(List<Record> records) throws Exception {
        for (Record record : records) {
            S3SourceRecord s3SourceRecord = (S3SourceRecord) record;
            String objectName = s3SourceRecord.name;
            log.info("Removing object {}", objectName);
            minioClient.removeObject(
                RemoveObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .build());
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

        @Override
        public Object key() {
            return null;
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
        private class S3RecordHeader implements Header {

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
        }
    }
}
