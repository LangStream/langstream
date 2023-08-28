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
package ai.langstream.agents.webcrawler;

import ai.langstream.agents.webcrawler.crawler.Document;
import ai.langstream.agents.webcrawler.crawler.StatusStorage;
import ai.langstream.agents.webcrawler.crawler.WebCrawler;
import ai.langstream.agents.webcrawler.crawler.WebCrawlerConfiguration;
import ai.langstream.agents.webcrawler.crawler.WebCrawlerStatus;
import ai.langstream.api.runner.code.AbstractAgentCode;
import ai.langstream.api.runner.code.AgentContext;
import ai.langstream.api.runner.code.AgentSource;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.BucketExistsArgs;
import io.minio.GetObjectArgs;
import io.minio.GetObjectResponse;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WebCrawlerSource extends AbstractAgentCode implements AgentSource {

    private int maxUnflushedPages = 100;

    private String bucketName;
    private Set<String> allowedDomains;
    private Set<String> seedUrls;
    private MinioClient minioClient;
    private int idleTime;

    private String statusFileName;

    private WebCrawler crawler;

    private boolean finished;

    private final AtomicInteger flushNext = new AtomicInteger(100);

    private final BlockingQueue<Document> foundDocuments = new LinkedBlockingQueue<>();

    private final StatusStorage statusStorage = new S3StatusStorage();

    @Override
    public void init(Map<String, Object> configuration) throws Exception {
        bucketName = configuration.getOrDefault("bucketName", "langstream-source").toString();
        String endpoint = configuration.getOrDefault("endpoint", "http://minio-endpoint.-not-set:9090").toString();
        String username =  configuration.getOrDefault("access-key", "minioadmin").toString();
        String password =  configuration.getOrDefault("secret-key", "minioadmin").toString();
        String region = configuration.getOrDefault("region", "").toString();
        allowedDomains = Set.of(configuration.getOrDefault("allowed-domains", "")
                .toString().split(","));
        seedUrls = Set.of(configuration.getOrDefault("seed-urls", "")
                .toString().split(","));
        idleTime = Integer.parseInt(configuration.getOrDefault("idle-time", 1).toString());
        maxUnflushedPages = Integer.parseInt(configuration.getOrDefault("max-unflushed-pages", 100).toString());
        flushNext.set(maxUnflushedPages);
        int minTimeBetweenRequests =
            Integer.parseInt(configuration.getOrDefault("min-time-between-requests", 100).toString());
        String userAgent = configuration.getOrDefault("user-agent", "langstream.ai-webcrawler/1.0").toString();

        log.info("Connecting to S3 Bucket at {} in region {} with user {}", endpoint, region, username);
        log.info("allowed-domains: {}", allowedDomains);
        log.info("seed-urls: {}", seedUrls);
        log.info("user-agent: {}", userAgent);
        log.info("max-unflushed-pages: {}", maxUnflushedPages);
        log.info("min-time-between-requests: {}", minTimeBetweenRequests);

        MinioClient.Builder builder = MinioClient.builder()
                .endpoint(endpoint)
                .credentials(username, password);
        if (!region.isBlank()) {
            builder.region(region);
        }
        minioClient = builder.build();

        makeBucketIfNotExists(bucketName);

        WebCrawlerConfiguration webCrawlerConfiguration = WebCrawlerConfiguration
                .builder()
                .allowedDomains(allowedDomains)
                .minTimeBetweenRequests(minTimeBetweenRequests)
                .userAgent(userAgent)
                .build();

        WebCrawlerStatus status = new WebCrawlerStatus();
        crawler = new WebCrawler(webCrawlerConfiguration, status, foundDocuments::add);



    }

    @Override
    public void setContext(AgentContext context) {
        String globalAgentId = context.getGlobalAgentId();
        statusFileName = globalAgentId + ".webcrawler.status.json";
        log.info("Status file is {}", statusFileName);
    }


    private void makeBucketIfNotExists(String bucketName)
        throws ServerException, InsufficientDataException, ErrorResponseException, IOException,
        NoSuchAlgorithmException, InvalidKeyException, InvalidResponseException, XmlParserException, InternalException {
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

    private void flushStatus() {
        try {
            crawler.getStatus().persist(statusStorage);
        } catch (Exception e) {
            log.error("Error persisting status", e);
        }
    }


    @Override
    public void start() throws Exception {
        crawler.getStatus().reloadFrom(statusStorage);

        for (String url :seedUrls) {
            crawler.crawl(url);
        }
    }

    @Override
    public List<Record> read() throws Exception {
        if (finished) {
            return sleepForNoResults();
        }
        if (foundDocuments.isEmpty()) {
            boolean somethingDone = crawler.runCycle();
            if (!somethingDone) {
                finished = true;
                log.info("No more documents found.");
                flushStatus();
            }
        }
        if (foundDocuments.isEmpty()) {
            return sleepForNoResults();
        }

        Document document = foundDocuments.remove();
        return List.of(new WebCrawlerSourceRecord(document.content().getBytes(StandardCharsets.UTF_8),
                document.url()));
    }

    private List<Record> sleepForNoResults() throws Exception {
        Thread.sleep(idleTime * 1000L);
        return List.of();
    }

    @Override
    protected Map<String, Object> buildAdditionalInfo() {
        return Map.of("bucketName", bucketName,
                "statusFileName", statusFileName,
                "seed-Urls", seedUrls,
                "allowed-domains", allowedDomains);
    }


    @Override
    public void commit(List<Record> records) {
        for (Record record : records) {
            WebCrawlerSourceRecord webCrawlerSourceRecord = (WebCrawlerSourceRecord) record;
            String objectName = webCrawlerSourceRecord.url;
            crawler.getStatus().urlProcessed(objectName);

            if (flushNext.decrementAndGet() == 0) {
                flushStatus();
                flushNext.set(maxUnflushedPages);
            }
        }
    }

    private static class WebCrawlerSourceRecord implements Record {
        private final byte[] read;
        private final String url;

        public WebCrawlerSourceRecord(byte[] read, String url) {
            this.read = read;
            this.url = url;
        }

        /**
         * the key is used for routing, so it is better to set it to something meaningful.
         * In case of retransmission the message will be sent to the same partition.
         * @return the key
         */
        @Override
        public Object key() {
            return url;
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
            return List.of(new SimpleRecord.SimpleHeader("url", url));
        }

        @Override
        public String toString() {
            return "WebCrawlerSourceRecord{" +
                    "url='" + url + '\'' +
                    '}';
        }
    }

    private class S3StatusStorage implements StatusStorage {
        private static final ObjectMapper MAPPER = new ObjectMapper();

        @Override
        public void storeStatus(Map<String, Object> metadata) throws Exception {
            byte[] content = MAPPER.writeValueAsBytes(metadata);
            log.info("Storing status in {}, {} bytes", statusFileName, content.length);
            minioClient.putObject(io.minio.PutObjectArgs.builder()
                    .bucket(bucketName)
                    .object(statusFileName)
                    .contentType("text/json")
                    .stream(new ByteArrayInputStream(content), content.length, -1)
                    .build());
        }

        @Override
        public Map<String, Object> getCurrentStatus() throws Exception {
            try {
                GetObjectResponse result = minioClient.getObject(GetObjectArgs.builder()
                        .bucket(bucketName)
                        .object(statusFileName)
                        .build());
                byte[] content = result.readAllBytes();
                log.info("Restoring status from {}, {} bytes", statusFileName, content.length);
                return MAPPER.readValue(content, Map.class);
            } catch (ErrorResponseException e) {
                if (e.errorResponse().code().equals("NoSuchKey")) {
                    log.info("No status file found, starting from scratch");
                    return Map.of();
                }
                throw e;
            }
        }
    }
}
