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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class WebCrawlerStatusTest {

    static final String URL1 = "https://site/page1";
    static final String URL2 = "https://site/page2";
    static final String URL2_ANCHOR = "https://site/page2#anchor";

    @Test
    public void testPreventCycles() {
        WebCrawlerStatus status = new WebCrawlerStatus();
        status.addUrl(URL1, true);
        verify(status, 1, 1, 1);
        status.addUrl(URL1, true);
        verify(status, 1, 1, 1);
        String url = status.nextUrl();
        verify(status, 1, 0, 1);
        status.urlProcessed(url);
        verify(status, 1, 0, 0);
        status.addUrl(URL1, true);
        verify(status, 1, 0, 0);
        status.addUrl(URL1, false);
        verify(status, 1, 0, 0);
    }

    @Test
    public void testPreventCyclesWithAnchors() {
        WebCrawlerStatus status = new WebCrawlerStatus();
        status.addUrl(URL2, true);
        verify(status, 1, 1, 1);
        status.addUrl(URL2_ANCHOR, true);
        verify(status, 1, 1, 1);
        String url = status.nextUrl();
        verify(status, 1, 0, 1);
        status.urlProcessed(url);
        verify(status, 1, 0, 0);
        status.addUrl(URL2, true);
        verify(status, 1, 0, 0);
        status.addUrl(URL2_ANCHOR, false);
        verify(status, 1, 0, 0);
    }



    @Test
    public void testReload() throws Exception {

        DummyStorage storage = new DummyStorage();

        WebCrawlerStatus status = new WebCrawlerStatus();
        status.addUrl(URL1, true);
        status.addUrl(URL2, true);
        verify(status, 2, 2, 2);
        status.persist(storage);

        status = new WebCrawlerStatus();
        status.reloadFrom(storage);
        verify(status, 2, 2, 2);
        String url = status.nextUrl();
        verify(status, 2, 1, 2);
        status.persist(storage);


        // restart after a crash, 1 page was not "committed"
        status = new WebCrawlerStatus();
        status.reloadFrom(storage);
        verify(status, 2, 2, 2);
        String url2 = status.nextUrl();

        // ensure that we restart from the same point
        assertEquals(url2, url);
        verify(status, 2, 1, 2);
        status.persist(storage);

        status.urlProcessed(url);
        verify(status, 2, 1, 1);
        status.persist(storage);

        status = new WebCrawlerStatus();
        status.reloadFrom(storage);
        verify(status, 2, 1, 1);

        url = status.nextUrl();
        status.urlProcessed(url);
        verify(status, 2, 0, 0);
        status.persist(storage);

        status = new WebCrawlerStatus();
        status.reloadFrom(storage);
        verify(status, 2, 0, 0);

    }

    private static void verify(WebCrawlerStatus status, int visited, int pending, int remaining) {
        assertEquals(pending, status.getPendingUrls().size());
        assertEquals(visited, status.getVisitedUrls().size());
        assertEquals(remaining, status.getRemainingUrls().size());
    }


    private static class DummyStorage implements StatusStorage {

        private Map<String, Object> lastMetadata;

        @Override
        public void storeStatus(Map<String, Object> metadata) {
            lastMetadata = new HashMap<>(metadata);
        }

        @Override
        public Map<String, Object> getCurrentStatus() {
            return lastMetadata != null ? new HashMap<>(lastMetadata) : Map.of();
        }
    }
}