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
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.util.Set;

@Slf4j
@Getter
public class WebCrawler {

    private final WebCrawlerConfiguration configuration;

    private final WebCrawlerStatus status;

    private final DocumentVisitor visitor;

    public WebCrawler(WebCrawlerConfiguration configuration,
                      WebCrawlerStatus status,
                      DocumentVisitor visitor) {
        this.configuration = configuration;
        this.visitor = visitor;
        this.status = status;
    }

    public void crawl(String startUrl) throws Exception {
        if (!configuration.isAllowedDomain(startUrl)) {
            return;
        }
        if (status.isVisited(startUrl)) {
            return;
        }
        status.addUrl(startUrl);
    }

    public boolean runCycle() throws Exception {
        String current = status.nextUrl();
        if (current == null) {
            return false;
        }
        Document document = Jsoup.connect(current).get();
        document.getElementsByAttribute("href").forEach(element -> {
            if (configuration.isAllowedTag(element.tagName())) {
                String url = element.absUrl("href");
                if (status.isVisited(url)) {
                    return;
                }
                if (configuration.isAllowedDomain(url)) {
                    System.out.println("Found url: " + url);
                    status.addUrl(url);
                } else {
                    System.out.println("Not allowed url: " + url);
                }
                status.addVisited(url);
            }
        });
        visitor.visit(new ai.langstream.agents.webcrawler.crawler.Document(current, document.html()));
        status.urlProcessed(current);
        return true;
    }


    public static void main(String ... args) throws Exception {
        WebCrawlerConfiguration configuration = WebCrawlerConfiguration
                .builder()
                .allowedDomains(Set.of("https://docs.langstream.ai/"))
                .build();

        WebCrawlerStatus status = new WebCrawlerStatus();
        WebCrawler crawler = new WebCrawler(configuration, status, new DocumentVisitor() {
            @Override
            public void visit(ai.langstream.agents.webcrawler.crawler.Document doc) {
                System.out.println("Visited: " + doc);
            }
        });
        crawler.crawl("https://docs.langstream.ai/");

        while (crawler.runCycle()) {
            System.out.println("Visited: " + status.getVisited().size());
            System.out.println("Remaining urls: " + status.getUrls().size());
            Thread.sleep(1000);
        }

        status.getVisited().forEach(url -> {
            System.out.println("Visited: " + url);
        });

    }


}
