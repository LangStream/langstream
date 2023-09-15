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

import crawlercommons.robots.SimpleRobotRules;
import crawlercommons.robots.SimpleRobotRulesParser;
import crawlercommons.sitemaps.AbstractSiteMap;
import crawlercommons.sitemaps.SiteMap;
import crawlercommons.sitemaps.SiteMapIndex;
import crawlercommons.sitemaps.SiteMapParser;
import crawlercommons.sitemaps.SiteMapURL;
import java.io.IOException;
import java.net.CookieManager;
import java.net.CookieStore;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jsoup.Connection;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.UnsupportedMimeTypeException;
import org.jsoup.nodes.Document;

@Slf4j
@Getter
public class WebCrawler {

    private final WebCrawlerConfiguration configuration;

    private final WebCrawlerStatus status;

    private final DocumentVisitor visitor;

    private final CookieStore cookieStore;

    public WebCrawler(
            WebCrawlerConfiguration configuration,
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
        if (!configuration.isAllowedUrl(startUrl)) {
            return;
        }

        // add robots.txt to the list of urls to crawl
        // before the original url
        // because it may contain some interesting information that will drive the crawling
        if (configuration.isHandleRobotsFile()) {
            try {
                URL url = new URL(startUrl);
                String path = url.getPath();
                if (path == null || path.isEmpty() || path.equals("/")) {
                    String separator = startUrl.endsWith("/") ? "" : "/";
                    String robotsFile = startUrl + separator + "robots.txt";
                    log.info("Adding robots.txt to the list of urls to crawl: {}", robotsFile);
                    // force add the url (no check on the max number of urls)
                    status.addUrl(robotsFile, URLReference.Type.ROBOTS, 0, true);
                }
            } catch (MalformedURLException e) {
                log.warn("Error while parsing the url: {}", startUrl, e);
            }
        }

        addUrl(startUrl, null);
    }

    private void addUrl(String startUrl, URLReference parent) {
        int size = status.getUrls().size();
        if (configuration.getMaxUrls() <= 0 || size < configuration.getMaxUrls()) {
            int newDepth = parent == null ? 0 : parent.depth() + 1;
            status.addUrl(startUrl, URLReference.Type.PAGE, newDepth, true);
        } else {
            log.info("Max urls reached, skipping {}", startUrl);
        }
    }

    public boolean runCycle() throws Exception {
        String current = status.nextUrl();
        if (current == null) {
            return false;
        }
        log.info("Crawling url: {}", current);

        URLReference reference = status.getReference(current);

        if (reference.type() == URLReference.Type.ROBOTS) {
            log.info("Found a robots.txt file");
            handleRobotsFile(current);
            status.urlProcessed(current);
            return true;
        }

        if (reference.type() == URLReference.Type.SITEMAP) {
            log.info("Found a sitemap file");
            handleSitemapsFile(current);
            status.urlProcessed(current);
            return true;
        }

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
                    if (!configuration.isAllowedUrl(location)) {
                        redirectedToForbiddenDomain = true;
                        log.warn(
                                "A redirection to a forbidden domain happened (from {} to {})",
                                current,
                                location);
                    } else {
                        log.info("A redirection happened from {} to {}", current, location);
                        addUrl(location, reference);
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
                    status.addUrl(current, reference.type(), reference.depth(), false);
                } else {
                    log.info("Putting back the url {} into the backlog", current);
                    status.addUrl(current, reference.type(), reference.depth(), true);
                }
            }

            // prevent from being banned for flooding
            if (configuration.getMinTimeBetweenRequests() > 0) {
                Thread.sleep(configuration.getMinTimeBetweenRequests());
            }

            // we did something
            return true;
        } catch (UnsupportedMimeTypeException notHtml) {
            log.info(
                    "Url {} lead to a {} content-type document. Skipping",
                    current,
                    notHtml.getMimeType());
            status.addUrl(current, reference.type(), reference.depth(), false);

            // prevent from being banned for flooding
            if (configuration.getMinTimeBetweenRequests() > 0) {
                Thread.sleep(configuration.getMinTimeBetweenRequests());
            }

            // we did something
            return true;
        }

        if (!redirectedToForbiddenDomain) {
            if (configuration.isScanHtmlDocuments()) {
                document.getElementsByAttribute("href")
                        .forEach(
                                element -> {
                                    if (configuration.isAllowedTag(element.tagName())) {
                                        String url = element.absUrl("href");
                                        if (configuration.isAllowedUrl(url)) {
                                            addUrl(url, reference);
                                        } else {
                                            log.debug("Ignoring not allowed url: {}", url);
                                            status.addUrl(
                                                    url,
                                                    reference.type(),
                                                    reference.depth(),
                                                    false);
                                        }
                                    }
                                });
            }
            visitor.visit(
                    new ai.langstream.agents.webcrawler.crawler.Document(current, document.html()));
        }

        // prevent from being banned for flooding
        if (configuration.getMinTimeBetweenRequests() > 0) {
            Thread.sleep(configuration.getMinTimeBetweenRequests());
        }

        return true;
    }

    private void handleRobotsFile(String url) throws Exception {
        HttpResponse<byte[]> response = downloadUrl(url);
        SimpleRobotRulesParser parser = new SimpleRobotRulesParser();
        SimpleRobotRules simpleRobotRules =
                parser.parseContent(
                        url,
                        response.body(),
                        response.headers().firstValue("content-type").orElse("text/plain"),
                        List.of(configuration.getUserAgent()));

        if (simpleRobotRules.getSitemaps().isEmpty()) {
            log.info("The robots.txt file doesn't contain any site map");
        } else {
            simpleRobotRules
                    .getSitemaps()
                    .forEach(
                            siteMap -> {
                                log.info("Adding sitemap : {}", siteMap);
                                // force add the url (no check on the max number of urls)
                                status.addUrl(siteMap, URLReference.Type.SITEMAP, 0, true);
                            });
        }
    }

    private void handleSitemapsFile(String url) throws Exception {
        HttpResponse<byte[]> response = downloadUrl(url);
        SiteMapParser siteMapParser = new SiteMapParser();
        AbstractSiteMap abstractSiteMap = siteMapParser.parseSiteMap(response.body(), new URL(url));
        if (abstractSiteMap instanceof SiteMap siteMap) {

            Collection<SiteMapURL> siteMapUrls = siteMap.getSiteMapUrls();

            siteMapUrls.forEach(
                    siteMapUrl -> {
                        log.info("Adding url from sitemap : {}", siteMapUrl.getUrl());
                        String loc = siteMapUrl.getUrl().toString();
                        if (configuration.isAllowedUrl(loc)) {
                            addUrl(loc, null);
                        } else {
                            log.debug("Ignoring not allowed url: {}", loc);
                            status.addUrl(loc, URLReference.Type.PAGE, 0, false);
                        }
                    });
        } else if (abstractSiteMap instanceof SiteMapIndex siteMapIndex) {
            log.info("It is a sitemap index");
            Collection<AbstractSiteMap> sitemaps = siteMapIndex.getSitemaps(true);
            sitemaps.forEach(
                    s -> {
                        log.info("Adding this sitemap : {}", s.getUrl());
                        status.addUrl(s.getUrl().toString(), URLReference.Type.SITEMAP, 0, true);
                    });
        } else {
            log.warn("Unknown sitemap type: {}", abstractSiteMap.getClass().getName());
        }
    }

    private HttpResponse<byte[]> downloadUrl(String url) throws IOException, InterruptedException {
        HttpClient client = HttpClient.newHttpClient();
        HttpResponse<byte[]> response =
                client.send(
                        HttpRequest.newBuilder()
                                .uri(URI.create(url))
                                .header("User-Agent", configuration.getUserAgent())
                                .build(),
                        HttpResponse.BodyHandlers.ofByteArray());
        return response;
    }

    public void restartIndexing(Set<String> seedUrls) {
        status.reset();
        for (String url : seedUrls) {
            crawl(url);
        }
        status.setLastIndexStartTimestamp(System.currentTimeMillis());
        status.setLastIndexEndTimestamp(0);
    }
}
