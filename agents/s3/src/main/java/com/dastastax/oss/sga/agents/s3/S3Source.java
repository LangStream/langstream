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
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class S3Source implements AgentSource {
    private String bucketName;
    private MinioClient minioClient;
    private final Set<String> objectsToCommit = ConcurrentHashMap.newKeySet();

    @Override
    public void init(Map<String, Object> configuration) throws Exception {
        bucketName = configuration.getOrDefault("bucketName", "sga-source").toString();
        String endpoint = configuration.getOrDefault("endpoint", "http://minio-endpoint.-not-set:9090").toString();
        String username =  configuration.getOrDefault("username", "minioadmin").toString();
        String password =  configuration.getOrDefault("password", "minioadmin").toString();

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
        for (Result<Item> object : minioClient.listObjects(ListObjectsArgs.builder().bucket(bucketName).build())) {
            String name = object.get().objectName();
            if (!objectsToCommit.contains(name)) {
                GetObjectResponse objectResponse = minioClient.getObject(
                    GetObjectArgs.builder()
                        .bucket(bucketName)
                        .object(name)
                        .build());
                String content = new String(objectResponse.readAllBytes(), StandardCharsets.UTF_8);
                objectsToCommit.add(name);
                records.add(new Record() {
                    @Override
                    public Object key() {
                        return null;
                    }

                    @Override
                    public Object value() {
                        return content;
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
                        return null;
                    }
                });
                break;
            }
        }
        return records;
    }

    @Override
    public void commit() throws Exception {
        for (Iterator<String> iterator = objectsToCommit.iterator(); iterator.hasNext(); ) {
            String objectName = iterator.next();
            minioClient.removeObject(
                RemoveObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .build());
            iterator.remove();
        }
    }
}
