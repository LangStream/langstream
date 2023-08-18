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
package ai.langstream.agents.webcrawler.crawler;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Getter
@Slf4j
public class WebCrawlerStatus {
    /**
     * List of all URLs discovered and to be processed.
     * An URL is removed from here on Source.commit()
     */
    private final Deque<String> remainingUrls = new ArrayDeque<>();

    /**
     * List of the URLs that are to be processed,
     * this Deque is used to keep track of the urls that have not been
     * returned by the Source yet.
     * An Url is removed from here on Source.read()
     */
    private final Deque<String> pendingUrls = new ArrayDeque<>();

    /**
     * Memory of all the URLs that have been seen by the Crawler in order
     * to prevent cycles.
     * This structure only grows and is never cleared.
     */
    private final Set<String> visitedUrls = new HashSet<>();

    public void reloadFrom(StatusStorage statusStorage) throws Exception {
        Map<String, Object> currentStatus = statusStorage.getCurrentStatus();
        if (currentStatus != null) {
            log.info("Found a saved status, reloading...");
            pendingUrls.clear();
            remainingUrls.clear();
            visitedUrls.clear();

            // please note that the order here is important
            // we want to visit the initial urls first
            List<String> remainingUrls = (List<String>) currentStatus.get("remainingUrls");

            if (remainingUrls != null) {
                log.info("Reloaded {} remaining urls", remainingUrls.size());
                remainingUrls.forEach(u -> {
                    log.info("Remaining {}", u);
                });
                this.pendingUrls.addAll(remainingUrls);
                this.remainingUrls.addAll(remainingUrls);
            }

            List<String> visitedUrls = (List<String>) currentStatus.get("visitedUrls");
            if (visitedUrls != null) {
                log.info("Reloaded {} visited urls", visitedUrls.size());
                visitedUrls.forEach(u -> {
                    log.info("Visited {}", u);
                });
                this.visitedUrls.addAll(visitedUrls);
            }
        } else {
            log.info("No saved status found, starting from scratch");
        }
    }

    public void persist(StatusStorage statusStorage) throws Exception {
        statusStorage.storeStatus(Map.of(
                "remainingUrls", new ArrayList<>(remainingUrls),
                "visitedUrls", new ArrayList<>(visitedUrls)
        ));
    }

    public void addUrl(String url, boolean toScan) {

        // the '#' character is used to identify a fragment in a URL
        // we have to remove it to avoid duplicates
        int hash = url.indexOf('#');
        if (hash >= 0) {
            url = url.substring(0, hash);
        }

        if (visitedUrls.contains(url)) {
            return;
        }
        visitedUrls.add(url);
        if (toScan) {
            log.info("adding url {} to list", url);
            pendingUrls.add(url);
            remainingUrls.add(url);
        }
    }

    public String nextUrl() {
        return pendingUrls.poll();
    }

    public void urlProcessed(String url) {
        // this method is called on "commit()", then the page has been successfully processed
        // downstream (for instance stored in the Vector database)
        log.info("Url {} completely processed", url);
        remainingUrls.remove(url);
    }
}
