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

import java.util.Set;

@Data
@Builder
public class WebCrawlerConfiguration {
    @Builder.Default
    private Set<String> allowedDomains = Set.of();
    private String userAgent;
    private int minTimeBetweenRequests = 100;
    @Builder.Default
    private Set<String> allowedTags = Set.of("a");

    public boolean isAllowedDomain(String url) {
        return allowedDomains.stream().anyMatch(url::startsWith);
    }

    public boolean isAllowedTag(String tagName) {
        return tagName != null && allowedTags.contains(tagName.toLowerCase());
    }
}
