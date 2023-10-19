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
package ai.langstream.agents.webcrawler.crawler;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
public class WebCrawlerStatus {

    /** Timestamp of the last index start. This is used to avoid reprocessing the indexing. */
    private long lastIndexStartTimestamp = 0;

    /** Timestamp of the last index end. This is used to avoid reprocessing the indexing. */
    private long lastIndexEndTimestamp = 0;

    /**
     * List of all URLs discovered and to be processed. An URL is removed from here on
     * Source.commit()
     */
    private final Deque<String> remainingUrls = new ArrayDeque<>();

    /**
     * List of the URLs that are to be processed, this Deque is used to keep track of the urls that
     * have not been returned by the Source yet. An Url is removed from here on Source.read()
     */
    private final Deque<String> pendingUrls = new ArrayDeque<>();

    /**
     * Memory of all the URLs that have been seen by the Crawler in order to prevent cycles. This
     * structure only grows and is never cleared.
     */
    private final Map<String, URLReference> urls = new HashMap<>();

    /**
     * Map of the robots.txt files that have been seen by the Crawler. This structure only grows and
     * is never cleared. The key is the website domain, the value is the robots.txt file.
     */
    private final Map<String, StatusStorage.RobotsFile> robotsFiles = new HashMap<>();

    /**
     * Map of the URLs that have been seen by the Crawler and that have returned a temporary error.
     * This status is not persisted.
     */
    private final Map<String, Integer> errorCount = new HashMap<>();

    public void reloadFrom(StatusStorage statusStorage) throws Exception {
        StatusStorage.Status currentStatus = statusStorage.getCurrentStatus();
        if (currentStatus != null) {
            log.info("Found a saved status, reloading...");
            pendingUrls.clear();
            remainingUrls.clear();
            urls.clear();

            // please note that the order here is important
            // we want to visit the initial urls first
            List<String> remainingUrls = currentStatus.remainingUrls();

            if (remainingUrls != null) {
                log.info("Reloaded {} remaining urls", remainingUrls.size());
                remainingUrls.forEach(u -> log.info("Remaining {}", u));
                this.pendingUrls.addAll(remainingUrls);
                this.remainingUrls.addAll(remainingUrls);
            }

            List<StatusStorage.StoreUrlReference> urls = currentStatus.urls();
            if (urls != null) {
                log.info("Reloaded {} urls", this.urls.size());
                urls.forEach(
                        u -> {
                            log.info("Visited {}", u);
                            String url = u.url();
                            String type = u.type();
                            int depth = u.depth();
                            URLReference reference =
                                    new URLReference(url, URLReference.Type.valueOf(type), depth);
                            this.urls.put(url, reference);
                        });
            }

            Long lastIndexEndTimestamp = currentStatus.lastIndexEndTimestamp();
            if (lastIndexEndTimestamp != null) {
                this.lastIndexEndTimestamp = lastIndexEndTimestamp;
            }
            Long lastIndexStartTimestamp = currentStatus.lastIndexStartTimestamp();
            if (lastIndexStartTimestamp != null) {
                this.lastIndexStartTimestamp = lastIndexStartTimestamp;
            }

            Map<String, StatusStorage.RobotsFile> robots = currentStatus.robotFiles();
            this.robotsFiles.clear();
            if (robots != null) {
                robotsFiles.putAll(robots);
            }
        } else {
            log.info("No saved status found, starting from scratch");
        }
    }

    public Map<String, StatusStorage.RobotsFile> getRobotsFiles() {
        return robotsFiles;
    }

    public void storeRobotsFile(String url, String robotsFile, String contentType) {
        robotsFiles.put(url, new StatusStorage.RobotsFile(robotsFile, contentType));
    }

    public long getLastIndexEndTimestamp() {
        return lastIndexEndTimestamp;
    }

    public void setLastIndexEndTimestamp(long lastIndexEndTimestamp) {
        this.lastIndexEndTimestamp = lastIndexEndTimestamp;
    }

    public long getLastIndexStartTimestamp() {
        return lastIndexStartTimestamp;
    }

    public void setLastIndexStartTimestamp(long lastIndexStartTimestamp) {
        this.lastIndexStartTimestamp = lastIndexStartTimestamp;
    }

    public void persist(StatusStorage statusStorage) throws Exception {
        List<StatusStorage.StoreUrlReference> urlReferencesForStore =
                urls.values().stream()
                        .map(
                                ref ->
                                        new StatusStorage.StoreUrlReference(
                                                ref.url(), ref.type().name(), ref.depth()))
                        .collect(Collectors.toList());
        statusStorage.storeStatus(
                new StatusStorage.Status(
                        new ArrayList<>(remainingUrls),
                        urlReferencesForStore,
                        lastIndexEndTimestamp,
                        lastIndexStartTimestamp,
                        new HashMap<>(robotsFiles)));
    }

    public void addUrl(String url, URLReference.Type type, int depth, boolean toScan) {

        // the '#' character is used to identify a fragment in a URL
        // we have to remove it to avoid duplicates
        url = removeFragment(url);

        boolean wasThere = urls.containsKey(url);
        // update the depth if the url was already there
        urls.put(url, new URLReference(url, type, depth));

        if (toScan && !wasThere) {
            if (log.isDebugEnabled()) {
                log.debug("adding url {} to list", url);
            }
            pendingUrls.add(url);
            remainingUrls.add(url);
        }
    }

    private static String removeFragment(String url) {
        int hash = url.indexOf('#');
        if (hash >= 0) {
            url = url.substring(0, hash);
        }
        return url;
    }

    public String nextUrl() {
        if (log.isDebugEnabled()) {
            log.debug("PendingUrls: {} Uncommitted {}", pendingUrls.size(), remainingUrls.size());
        }
        return pendingUrls.poll();
    }

    public void urlProcessed(String url) {
        // this method is called on "commit()", then the page has been successfully processed
        // downstream (for instance stored in the Vector database)
        if (log.isDebugEnabled()) {
            log.debug("Url {} completely processed", url);
        }
        remainingUrls.remove(url);

        // forget the errors about the page
        url = removeFragment(url);
        errorCount.remove(url);
    }

    public int temporaryErrorOnUrl(String url) {
        url = removeFragment(url);
        urls.remove(url);
        return errorCount.compute(
                url,
                (u, current) -> {
                    if (current == null) {
                        return 1;
                    } else {
                        return current + 1;
                    }
                });
    }

    public void reset() {
        urls.clear();
        errorCount.clear();
        pendingUrls.clear();
        remainingUrls.clear();
        robotsFiles.clear();
    }

    public URLReference getReference(String current) {
        URLReference reference = urls.get(current);
        if (reference == null) {
            throw new IllegalStateException("Unknown url " + current);
        }
        return reference;
    }
}
