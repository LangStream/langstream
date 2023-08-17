package ai.langstream.agents.webcrawler.crawler;

import lombok.Builder;
import lombok.Data;

import java.util.Set;

@Data
@Builder
public class WebCrawlerConfiguration {
    @Builder.Default
    private Set<String> allowedDomains = Set.of();
    @Builder.Default
    private Set<String> allowedTags = Set.of("a");

    public boolean isAllowedDomain(String url) {
        return allowedDomains.stream().anyMatch(url::startsWith);
    }

    public boolean isAllowedTag(String tagName) {
        return tagName != null && allowedTags.contains(tagName.toLowerCase());
    }
}
