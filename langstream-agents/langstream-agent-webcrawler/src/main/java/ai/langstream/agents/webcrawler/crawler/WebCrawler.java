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
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.net.CookieManager;
import java.net.CookieStore;

@Slf4j
@Getter
public class WebCrawler {

    private final WebCrawlerConfiguration configuration;

    private final WebCrawlerStatus status;

    private final DocumentVisitor visitor;

    private final CookieStore cookieStore;

    public WebCrawler(WebCrawlerConfiguration configuration,
                      WebCrawlerStatus status,
                      DocumentVisitor visitor) {
        this.configuration = configuration;
        this.visitor = visitor;
        this.status = status;

        CookieManager cookieManager = new CookieManager();
        cookieManager.setCookiePolicy(java.net.CookiePolicy.ACCEPT_ALL);
        this.cookieStore = cookieManager.getCookieStore();
    }

    public void crawl(String startUrl) {
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
        log.info("Crawling url: {}", current);
        Connection connect = Jsoup.connect(current);
        connect.cookieStore(cookieStore);
        connect.followRedirects(false);
        if (configuration.getUserAgent() != null) {
            connect.userAgent(configuration.getUserAgent());
        }
        connect.timeout(configuration.getHttpTimeout());

        boolean redirectedToForbiddenDomain = false;
        Document document;
        try {
            document = connect.get();
            Connection.Response response = connect.response();
            int statusCode = response.statusCode();
            if (statusCode >= 300 && statusCode < 400) {
                String location = response.header("Location");
                if (!location.equals(current)) {
                    if (!configuration.isAllowedDomain(location)) {
                        redirectedToForbiddenDomain = true;
                        log.warn("A redirection to a forbidden domain happened (from {} to {})", current, location);
                    } else {
                        log.info("A redirection happened from {} to {}", current, location);
                        status.addUrl(location, true);
                        return true;
                    }
                }
            }
        } catch (HttpStatusException e) {
            int code = e.getStatusCode();
            log.info("Error while crawling url: {}, HTTP code {}", current, code);

            if (code >= 400 && code < 500) {
                // not found, forbidden...this is a fatal error
                log.info("Skipping the url {}", current);
            } else if (code < 400 || (code >= 500 && code < 600)) {
                // 1xx, 2xx, 3xx...this is not expected as it is not an "ERROR"
                // 5xx errors are server side errors, we can retry

                int currentCount = status.temporaryErrorOnUrl(current);
                if (currentCount >= configuration.getMaxErrorCount()) {
                    log.info("Too many errors ({}) on url {}, skipping it", currentCount, current);
                    status.addUrl(current, false);
                } else {
                    log.info("Putting back the url {} into the backlog", current);
                    status.addUrl(current, true);
                }
            }

            // prevent from being banned for flooding
            if (configuration.getMinTimeBetweenRequests() > 0) {
                Thread.sleep(configuration.getMinTimeBetweenRequests());
            }

            // we did something
            return true;
        }
        try {

            if (!redirectedToForbiddenDomain) {
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
            }
        } catch (RuntimeException error) {
            log.error("Error while processing url: {}", current, error);
            status.addUrl(current, false);
        }

        // prevent from being banned for flooding
        if (configuration.getMinTimeBetweenRequests() > 0) {
            Thread.sleep(configuration.getMinTimeBetweenRequests());
        }

        return true;
    }

}
