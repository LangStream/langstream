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
package ai.langstream.agents.webcrawler;

import static ai.langstream.agents.webcrawler.crawler.WebCrawlerConfiguration.DEFAULT_USER_AGENT;
import static ai.langstream.api.util.ConfigurationUtils.getBoolean;
import static ai.langstream.api.util.ConfigurationUtils.getInt;
import static ai.langstream.api.util.ConfigurationUtils.getSet;
import static ai.langstream.api.util.ConfigurationUtils.getString;

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
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WebCrawlerSource extends AbstractAgentCode implements AgentSource {

    private int maxUnflushedPages = 100;

    private String bucketName;
    private Set<String> allowedDomains;
    private Set<String> forbiddenPaths;
    private int maxUrls;
    private boolean handleRobotsFile;
    private boolean scanHtmlDocuments;
    private Set<String> seedUrls;
    private Map<String, Object> agentConfiguration;
    private MinioClient minioClient;
    private int reindexIntervalSeconds;

    @Getter private String statusFileName;
    Optional<Path> localDiskPath;

    private WebCrawler crawler;

    private boolean finished;

    private final AtomicInteger flushNext = new AtomicInteger(100);

    private final BlockingQueue<Document> foundDocuments = new LinkedBlockingQueue<>();

    private StatusStorage statusStorage;

    private Runnable onReindexStart;

    public Runnable getOnReindexStart() {
        return onReindexStart;
    }

    public void setOnReindexStart(Runnable onReindexStart) {
        this.onReindexStart = onReindexStart;
    }

    @Override
    public void init(Map<String, Object> configuration) throws Exception {
        agentConfiguration = configuration;

        allowedDomains = getSet("allowed-domains", configuration);
        forbiddenPaths = getSet("forbidden-paths", configuration);
        maxUrls = getInt("max-urls", 1000, configuration);
        int maxDepth = getInt("max-depth", 50, configuration);
        handleRobotsFile = getBoolean("handle-robots-file", true, configuration);
        scanHtmlDocuments = getBoolean("scan-html-documents", true, configuration);
        seedUrls = getSet("seed-urls", configuration);
        reindexIntervalSeconds = getInt("reindex-interval-seconds", 60 * 60 * 24, configuration);
        maxUnflushedPages = getInt("max-unflushed-pages", 100, configuration);

        flushNext.set(maxUnflushedPages);
        int minTimeBetweenRequests = getInt("min-time-between-requests", 500, configuration);
        String userAgent = getString("user-agent", DEFAULT_USER_AGENT, configuration);
        int maxErrorCount = getInt("max-error-count", 5, configuration);
        int httpTimeout = getInt("http-timeout", 10000, configuration);
        boolean allowNonHtmlContents = getBoolean("allow-non-html-contents", false, configuration);

        boolean handleCookies = getBoolean("handle-cookies", true, configuration);

        log.info("allowed-domains: {}", allowedDomains);
        log.info("forbidden-paths: {}", forbiddenPaths);
        log.info("allow-non-html-contents: {}", allowNonHtmlContents);
        log.info("seed-urls: {}", seedUrls);
        log.info("max-urls: {}", maxUrls);
        log.info("max-depth: {}", maxDepth);
        log.info("handle-robots-file: {}", handleRobotsFile);
        log.info("scan-html-documents: {}", scanHtmlDocuments);
        log.info("user-agent: {}", userAgent);
        log.info("max-unflushed-pages: {}", maxUnflushedPages);
        log.info("min-time-between-requests: {}", minTimeBetweenRequests);
        log.info("reindex-interval-seconds: {}", reindexIntervalSeconds);

        WebCrawlerConfiguration webCrawlerConfiguration =
                WebCrawlerConfiguration.builder()
                        .allowedDomains(allowedDomains)
                        .allowNonHtmlContents(allowNonHtmlContents)
                        .maxUrls(maxUrls)
                        .maxDepth(maxDepth)
                        .forbiddenPaths(forbiddenPaths)
                        .handleRobotsFile(handleRobotsFile)
                        .minTimeBetweenRequests(minTimeBetweenRequests)
                        .userAgent(userAgent)
                        .handleCookies(handleCookies)
                        .httpTimeout(httpTimeout)
                        .maxErrorCount(maxErrorCount)
                        .build();

        WebCrawlerStatus status = new WebCrawlerStatus();
        // this can be overwritten when the status is reloaded
        status.setLastIndexStartTimestamp(System.currentTimeMillis());
        crawler = new WebCrawler(webCrawlerConfiguration, status, foundDocuments::add);
    }

    @Override
    public void setContext(AgentContext context) throws Exception {
        super.setContext(context);
        String globalAgentId = context.getGlobalAgentId();
        statusFileName = globalAgentId + ".webcrawler.status.json";
        log.info("Status file is {}", statusFileName);
        final String agentId = agentId();
        localDiskPath = context.getPersistentStateDirectoryForAgent(agentId);
        String stateStorage = getString("state-storage", "s3", agentConfiguration);
        if (stateStorage.equals("disk")) {
            if (!localDiskPath.isPresent()) {
                throw new IllegalArgumentException(
                        "No local disk path available for agent "
                                + agentId
                                + " and state-storage was set to 'disk'");
            }
            log.info("Using local disk storage");

            statusStorage = new LocalDiskStatusStorage();
        } else {
            log.info("Using S3 storage");
            bucketName = getString("bucketName", "langstream-source", agentConfiguration);
            String endpoint =
                    getString(
                            "endpoint", "http://minio-endpoint.-not-set:9090", agentConfiguration);
            String username = getString("access-key", "minioadmin", agentConfiguration);
            String password = getString("secret-key", "minioadmin", agentConfiguration);
            String region = getString("region", "", agentConfiguration);

            log.info(
                    "Connecting to S3 Bucket at {} in region {} with user {}",
                    endpoint,
                    region,
                    username);

            MinioClient.Builder builder =
                    MinioClient.builder().endpoint(endpoint).credentials(username, password);
            if (!region.isBlank()) {
                builder.region(region);
            }
            minioClient = builder.build();

            makeBucketIfNotExists(bucketName);
            statusStorage = new S3StatusStorage();
        }
    }

    @SneakyThrows
    private void makeBucketIfNotExists(String bucketName) {
        if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build())) {
            log.info("Creating bucket {}", bucketName);
            minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
        } else {
            log.info("Bucket {} already exists", bucketName);
        }
    }

    public WebCrawler getCrawler() {
        return crawler;
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
        crawler.reloadStatus(statusStorage);

        for (String url : seedUrls) {
            crawler.crawl(url);
        }
    }

    @Override
    public List<Record> read() throws Exception {
        if (finished) {
            checkReindexIsNeeded();
            return sleepForNoResults();
        }
        if (foundDocuments.isEmpty()) {
            boolean somethingDone = crawler.runCycle();
            if (!somethingDone) {
                finished = true;
                log.info("No more documents found.");
                crawler.getStatus().setLastIndexEndTimestamp(System.currentTimeMillis());
                if (reindexIntervalSeconds > 0) {
                    Instant next =
                            Instant.ofEpochMilli(crawler.getStatus().getLastIndexEndTimestamp())
                                    .plusSeconds(reindexIntervalSeconds);
                    log.info(
                            "Next re-index will happen in {} seconds, at {}",
                            reindexIntervalSeconds,
                            next);
                }
                flushStatus();
            } else {
                // we did something but no new documents were found (for instance a redirection has
                // been processed)
                // no need to sleep
                if (foundDocuments.isEmpty()) {
                    log.info("The last cycle didn't produce any new documents");
                    return List.of();
                }
            }
        }
        if (foundDocuments.isEmpty()) {
            return sleepForNoResults();
        }

        Document document = foundDocuments.remove();
        processed(0, 1);
        return List.of(
                new WebCrawlerSourceRecord(
                        document.content(), document.url(), document.contentType()));
    }

    private void checkReindexIsNeeded() {
        if (reindexIntervalSeconds <= 0) {
            return;
        }
        long lastIndexEndTimestamp = crawler.getStatus().getLastIndexEndTimestamp();
        if (lastIndexEndTimestamp <= 0) {
            // indexing is not finished yet
            log.debug("Reindexing is not needed, indexing is not finished yet");
            return;
        }
        long now = System.currentTimeMillis();
        long elapsedSeconds = (now - lastIndexEndTimestamp) / 1000;
        if (elapsedSeconds >= reindexIntervalSeconds) {
            if (onReindexStart != null) {
                // for tests
                onReindexStart.run();
            }
            log.info(
                    "Reindexing is needed, last index end timestamp is {}, {} seconds ago",
                    Instant.ofEpochMilli(lastIndexEndTimestamp),
                    elapsedSeconds);
            crawler.restartIndexing(seedUrls);
            finished = false;
            flushStatus();
        } else {
            log.debug(
                    "Reindexing is not needed, last end start timestamp is {}, {} seconds ago",
                    Instant.ofEpochMilli(lastIndexEndTimestamp),
                    elapsedSeconds);
        }
    }

    private List<Record> sleepForNoResults() throws Exception {
        Thread.sleep(100);
        return List.of();
    }

    @Override
    protected Map<String, Object> buildAdditionalInfo() {
        Map<String, Object> additionalInfo = new HashMap<>();
        additionalInfo.put("seed-Urls", seedUrls);
        additionalInfo.put("allowed-domains", allowedDomains);
        additionalInfo.put("statusFileName", statusFileName);
        additionalInfo.put("bucketName", bucketName);
        return additionalInfo;
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
        private final String contentType;

        public WebCrawlerSourceRecord(byte[] read, String url, String contentType) {
            this.read = read;
            this.url = url;
            this.contentType = contentType;
        }

        /**
         * the key is used for routing, so it is better to set it to something meaningful. In case
         * of retransmission the message will be sent to the same partition.
         *
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
            return List.of(
                    new SimpleRecord.SimpleHeader("url", url),
                    new SimpleRecord.SimpleHeader("content_type", contentType));
        }

        @Override
        public String toString() {
            return "WebCrawlerSourceRecord{" + "url='" + url + '\'' + '}';
        }
    }

    private class S3StatusStorage implements StatusStorage {
        private static final ObjectMapper MAPPER = new ObjectMapper();

        @Override
        public void storeStatus(Status status) throws Exception {
            byte[] content = MAPPER.writeValueAsBytes(status);
            log.info("Storing status in {}, {} bytes", statusFileName, content.length);
            minioClient.putObject(
                    io.minio.PutObjectArgs.builder()
                            .bucket(bucketName)
                            .object(statusFileName)
                            .contentType("text/json")
                            .stream(new ByteArrayInputStream(content), content.length, -1)
                            .build());
        }

        @Override
        public Status getCurrentStatus() throws Exception {
            try {
                GetObjectResponse result =
                        minioClient.getObject(
                                GetObjectArgs.builder()
                                        .bucket(bucketName)
                                        .object(statusFileName)
                                        .build());
                byte[] content = result.readAllBytes();
                log.info("Restoring status from {}, {} bytes", statusFileName, content.length);
                try {
                    return MAPPER.readValue(content, Status.class);
                } catch (IOException e) {
                    log.error("Error parsing status file", e);
                    return null;
                }
            } catch (ErrorResponseException e) {
                if (e.errorResponse().code().equals("NoSuchKey")) {
                    return new Status(List.of(), List.of(), null, null, Map.of());
                }
                throw e;
            }
        }
    }

    private class LocalDiskStatusStorage implements StatusStorage {
        private static final ObjectMapper MAPPER = new ObjectMapper();

        @Override
        public void storeStatus(Status status) throws Exception {
            final Path fullPath = computeFullPath();
            log.info("Storing status to the disk at path {}", fullPath);
            MAPPER.writeValue(fullPath.toFile(), status);
        }

        private Path computeFullPath() {
            final Path fullPath = localDiskPath.get().resolve(statusFileName);
            return fullPath;
        }

        @Override
        public Status getCurrentStatus() throws Exception {
            final Path fullPath = computeFullPath();
            if (Files.exists(fullPath)) {
                log.info("Restoring status from {}", fullPath);
                try {
                    return MAPPER.readValue(fullPath.toFile(), Status.class);
                } catch (IOException e) {
                    log.error("Error parsing status file", e);
                    return null;
                }
            } else {
                return null;
            }
        }
    }
}
