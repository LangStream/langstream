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

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class WebCrawlerStatusTest {

    static final String URL1 = "https://site/page1";
    static final String URL2 = "https://site/page2";
    static final String URL2_ANCHOR = "https://site/page2#anchor";
    static final String URL3 = "https://site/page3";
    static final String URL4 = "https://site/page4";
    static final String URL5 = "https://site/page5";

    @Test
    public void testPreventCycles() {
        WebCrawlerStatus status = new WebCrawlerStatus();
        status.addUrl(URL1, URLReference.Type.PAGE, 0, true);
        verify(status, 1, 1, 1);
        status.addUrl(URL1, URLReference.Type.PAGE, 0, true);
        verify(status, 1, 1, 1);
        String url = status.nextUrl();
        verify(status, 1, 0, 1);
        status.urlProcessed(url);
        verify(status, 1, 0, 0);
        status.addUrl(URL1, URLReference.Type.PAGE, 0, true);
        verify(status, 1, 0, 0);
        status.addUrl(URL1, URLReference.Type.PAGE, 0, false);
        verify(status, 1, 0, 0);
    }

    @Test
    public void testPreventCyclesWithAnchors() {
        WebCrawlerStatus status = new WebCrawlerStatus();
        status.addUrl(URL2, URLReference.Type.PAGE, 0, true);
        verify(status, 1, 1, 1);
        status.addUrl(URL2_ANCHOR, URLReference.Type.PAGE, 0, true);
        verify(status, 1, 1, 1);
        String url = status.nextUrl();
        verify(status, 1, 0, 1);
        status.urlProcessed(url);
        verify(status, 1, 0, 0);
        status.addUrl(URL2, URLReference.Type.PAGE, 0, true);
        verify(status, 1, 0, 0);
        status.addUrl(URL2_ANCHOR, URLReference.Type.PAGE, 0, false);
        verify(status, 1, 0, 0);
    }

    @Test
    public void testMaxUrls() throws Exception {
        WebCrawlerConfiguration configuration =
                WebCrawlerConfiguration.builder().maxUrls(2).build();
        WebCrawlerStatus status = new WebCrawlerStatus();
        WebCrawler webCrawler = new WebCrawler(configuration, status, (__) -> {});
        assertTrue(webCrawler.addPageUrl(URL1, null));
        assertTrue(webCrawler.addPageUrl(URL2, null));
        assertFalse(webCrawler.addPageUrl(URL3, null));

        verify(status, 2, 2, 2);
    }

    @Test
    public void testMaxDepth() {
        WebCrawlerConfiguration configuration =
                WebCrawlerConfiguration.builder().maxDepth(2).build();
        WebCrawlerStatus status = new WebCrawlerStatus();
        WebCrawler webCrawler = new WebCrawler(configuration, status, (__) -> {});
        assertTrue(webCrawler.addPageUrl(URL1, null));
        assertTrue(webCrawler.addPageUrl(URL2, status.getReference(URL1)));
        assertTrue(webCrawler.addPageUrl(URL3, status.getReference(URL2)));

        // coming from URL3, forbidden
        assertFalse(webCrawler.addPageUrl(URL4, status.getReference(URL3)));
        // coming from URL2, allowed
        assertTrue(webCrawler.addPageUrl(URL5, status.getReference(URL2)));

        // coming from URL2, allowed
        assertTrue(webCrawler.addPageUrl(URL4, status.getReference(URL2)));

        assertEquals(2, status.getReference(URL5).depth());
        // now page 5 is found from URL1, so depth is changed
        assertTrue(webCrawler.addPageUrl(URL5, status.getReference(URL1)));

        assertEquals(1, status.getReference(URL5).depth());

        verify(status, 5, 5, 5);
    }

    @Test
    public void testReload() throws Exception {

        DummyStorage storage = new DummyStorage();

        WebCrawlerStatus status = new WebCrawlerStatus();
        status.addUrl(URL1, URLReference.Type.PAGE, 0, true);
        status.addUrl(URL2, URLReference.Type.PAGE, 0, true);
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
        assertEquals(visited, status.getUrls().size());
        assertEquals(remaining, status.getRemainingUrls().size());
    }

    private static class DummyStorage implements StatusStorage {

        private Status lastMetadata;

        @Override
        public void storeStatus(Status metadata) {
            lastMetadata = metadata;
        }

        @Override
        public Status getCurrentStatus() {
            return lastMetadata != null
                    ? lastMetadata
                    : new Status(List.of(), List.of(), null, null, Map.of());
        }
    }
}
