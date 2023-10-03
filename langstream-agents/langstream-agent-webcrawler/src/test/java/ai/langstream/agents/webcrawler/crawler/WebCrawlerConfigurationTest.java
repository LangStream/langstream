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

import java.util.Set;
import org.junit.jupiter.api.Test;

class WebCrawlerConfigurationTest {
    @Test
    void testAllowedDomains() {
        assertTrue(verifyDomain("http://domain/something/....", Set.of("domain")));
        assertTrue(verifyDomain("https://domain/something/....", Set.of("domain")));
        assertTrue(verifyDomain("https://domain/something/....", Set.of("https://domain")));

        assertFalse(verifyDomain("https://domain/something/....", Set.of("https://domain/else")));
        assertFalse(verifyDomain("not-an-url", Set.of("domain")));
        assertFalse(verifyDomain("http://domain/something/....", Set.of()));
    }

    @Test
    void testForbiddenPaths() {
        // no forbidden paths
        assertTrue(verifyForbiddenPaths("http://domain/something/something", Set.of()));

        assertTrue(verifyForbiddenPaths("https://domain/something", Set.of("/something/")));
        assertTrue(
                verifyForbiddenPaths(
                        "https://domain/something/secondlevel", Set.of("/something-else")));
        assertTrue(
                verifyForbiddenPaths(
                        "https://domain/something/secondlevel", Set.of("/secondlevel")));

        assertFalse(verifyForbiddenPaths("https://domain/something/", Set.of("/something/")));
        assertFalse(
                verifyForbiddenPaths("https://domain/something/secondlevel", Set.of("/something")));
        assertFalse(
                verifyForbiddenPaths(
                        "https://domain/something/secondlevel", Set.of("/something/sec")));

        assertFalse(verifyForbiddenPaths("not-an-url", Set.of("/something")));

        // this is not an URL
        assertFalse(verifyForbiddenPaths("something:somewhere", Set.of("/something")));

        assertTrue(verifyForbiddenPaths("https://domain", Set.of("/something")));
        assertTrue(verifyForbiddenPaths("https://domain/", Set.of("/something")));
        assertFalse(verifyForbiddenPaths("https://domain", Set.of("/")));
        assertFalse(verifyForbiddenPaths("https://domain/", Set.of("/")));
    }

    private boolean verifyDomain(String url, Set<String> allowedDomains) {
        WebCrawlerConfiguration configuration =
                WebCrawlerConfiguration.builder().allowedDomains(allowedDomains).build();
        return configuration.isAllowedUrl(url);
    }

    private boolean verifyForbiddenPaths(String url, Set<String> forbidden) {
        WebCrawlerConfiguration configuration =
                WebCrawlerConfiguration.builder()
                        .allowedDomains(Set.of("domain"))
                        .forbiddenPaths(forbidden)
                        .build();
        return configuration.isAllowedUrl(url);
    }
}
