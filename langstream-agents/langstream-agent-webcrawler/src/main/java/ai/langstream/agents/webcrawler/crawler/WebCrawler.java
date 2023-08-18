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
import org.jsoup.Connection;
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
        status.addUrl(startUrl, true);
    }

    public boolean runCycle() throws Exception {
        String current = status.nextUrl();
        if (current == null) {
            return false;
        }
        Connection connect = Jsoup.connect(current);
        if (configuration.getUserAgent() != null) {
            connect.userAgent(configuration.getUserAgent());
        }
        Document document = connect.get();
        document.getElementsByAttribute("href").forEach(element -> {
            if (configuration.isAllowedTag(element.tagName())) {
                String url = element.absUrl("href");
                if (configuration.isAllowedDomain(url)) {
                    status.addUrl(url, true);
                } else {
                    log.info("Ignoring not allowed url: {}", url);
                    status.addUrl(url, false);
                }
            }
        });
        visitor.visit(new ai.langstream.agents.webcrawler.crawler.Document(current, document.html()));

        // prevent from being banned for flooding
        if (configuration.getMinTimeBetweenRequests() > 0) {
            Thread.sleep(configuration.getMinTimeBetweenRequests());
        }

        return true;
    }

}
