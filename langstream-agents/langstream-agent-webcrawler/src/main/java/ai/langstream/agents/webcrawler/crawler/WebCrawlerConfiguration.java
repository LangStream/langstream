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

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.util.Set;

@Data
@Builder
@Slf4j
public class WebCrawlerConfiguration {
    @Builder.Default
    private Set<String> allowedDomains = Set.of();
    @Builder.Default
    private String userAgent = null;
    @Builder.Default
    private int minTimeBetweenRequests = 100;
    @Builder.Default
    private int httpTimeout = 10000;
    @Builder.Default
    private int maxErrorCount = 5;
    @Builder.Default
    private boolean handleCookies = true;
    @Builder.Default
    private Set<String> allowedTags = Set.of("a");

    public boolean isAllowedDomain(String url) {
        final String domainOnly;
        try {
            URI uri = URI.create(url);
            String host = uri.getHost();
            domainOnly = host;
        } catch (Exception e) {
            log.info("Url {} doesn't have a domain, parsing error: {}", url, e + "");
            return false;
        }
        return allowedDomains
                .stream()
                .anyMatch(domain -> {
                    // the domain can be a dns name with the scheme and a part of the uri
                    // https://domain/something/....
                    // http://domain/....
                    if (url.startsWith(domain)) {
                        return true;
                    }
                    // or just the domain
                    return domain.equalsIgnoreCase(domainOnly);
                });
    }

    public boolean isAllowedTag(String tagName) {
        return tagName != null && allowedTags.contains(tagName.toLowerCase());
    }
}
