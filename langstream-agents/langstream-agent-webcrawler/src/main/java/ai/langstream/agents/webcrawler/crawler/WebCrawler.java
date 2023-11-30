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
import java.net.CookiePolicy;
import java.net.CookieStore;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

    private final Map<String, SimpleRobotRules> robotsRules = new HashMap<>();

    public WebCrawler(
            WebCrawlerConfiguration configuration,
            WebCrawlerStatus status,
            DocumentVisitor visitor) {
        this.configuration = configuration;
        this.visitor = visitor;
        this.status = status;

        CookieManager cookieManager = new CookieManager();
        cookieManager.setCookiePolicy(
                configuration.isHandleCookies()
                        ? CookiePolicy.ACCEPT_ALL
                        : CookiePolicy.ACCEPT_NONE);
        this.cookieStore = cookieManager.getCookieStore();
    }

    public void crawl(String startUrl) {
        if (isUrlForbidden(startUrl)) {
            return;
        }

        // add robots.txt to the list of urls to crawl
        // before the original url
        // because it may contain some interesting information that will drive the crawling
        if (configuration.isHandleRobotsFile()) {
            try {
                URL url = new URL(startUrl);
                String robotsFile =
                        url.getProtocol()
                                + "://"
                                + url.getHost()
                                + (url.getPort() > 0 ? ":" + url.getPort() : "")
                                + "/robots.txt";
                log.info("Adding robots.txt to the list of urls to crawl: {}", robotsFile);
                // force add the url (no check on the max number of urls)
                forceAddUrl(robotsFile, URLReference.Type.ROBOTS, 0);

            } catch (MalformedURLException e) {
                log.warn("Error while parsing the url: {}", startUrl, e);
            }
        }

        addPageUrl(startUrl, null);
    }

    void discardUrl(String current, URLReference reference) {
        status.addUrl(current, reference.type(), reference.depth(), false);
    }

    void forceAddUrl(String url, URLReference.Type type, int depth) {
        status.addUrl(url, type, depth, true);
    }

    boolean addPageUrl(String startUrl, URLReference parent) {
        int size = status.getUrls().size();
        int newDepth = parent == null ? 0 : parent.depth() + 1;
        if (configuration.getMaxUrls() > 0 && size >= configuration.getMaxUrls()) {
            log.info("Max urls reached, skipping {}", startUrl);
            return false;
        } else if (configuration.getMaxDepth() > 0 && newDepth > configuration.getMaxDepth()) {
            log.info("Max depth reached, skipping {}", startUrl);
            return false;
        } else {
            status.addUrl(startUrl, URLReference.Type.PAGE, newDepth, true);
            return true;
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
        Document document = null;
        String contentType = null;
        byte[] binaryContent = null;
        try {
            document = connect.get();
            Connection.Response response = connect.response();
            contentType = response.contentType();
            int statusCode = response.statusCode();
            if (statusCode >= 300 && statusCode < 400) {
                String location = response.header("Location");
                if (!Objects.equals(location, current)) {
                    if (isUrlForbidden(location)) {
                        redirectedToForbiddenDomain = true;
                        log.warn(
                                "A redirection to a forbidden domain happened (from {} to {})",
                                current,
                                location);
                    } else {
                        log.info("A redirection happened from {} to {}", current, location);
                        addPageUrl(location, reference);
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

                handleTemporaryError(current, reference);
            }

            handleThrottling(current);

            // we did something
            return true;
        } catch (UnsupportedMimeTypeException notHtml) {
            if (configuration.isAllowNonHtmlContents()) {
                log.info(
                        "Url {} lead to a {} content-type document. allow-not-html-content is true, so we are processing it",
                        current,
                        notHtml.getMimeType());
                handleThrottling(current);

                // download again the file, this is a little inefficient but currently
                // this is not the most common case, we can improve it later

                // downloadUrl takes care of retrying
                HttpResponse<byte[]> httpResponse = downloadUrl(current);
                contentType =
                        httpResponse
                                .headers()
                                .firstValue("content-type")
                                .orElse("application/octet-stream");
                binaryContent = httpResponse.body();
                visitor.visit(
                        new ai.langstream.agents.webcrawler.crawler.Document(
                                current, binaryContent, contentType));

                handleThrottling(current);

                return true;
            } else {
                log.info(
                        "Url {} lead to a {} content-type document. Skipping",
                        current,
                        notHtml.getMimeType());
                discardUrl(current, reference);

                // prevent from being banned for flooding
                handleThrottling(current);

                // we did something
                return true;
            }
        } catch (IOException e) {
            log.info("Error while crawling url: {}, IO Error: {}", current, e + "");

            handleTemporaryError(current, reference);

            // prevent from being banned for flooding
            handleThrottling(current);

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
                                        if (isUrlForbidden(url)) {
                                            log.debug("Ignoring not allowed url: {}", url);
                                            discardUrl(url, reference);
                                        } else {
                                            addPageUrl(url, reference);
                                        }
                                    }
                                });
            }
            visitor.visit(
                    new ai.langstream.agents.webcrawler.crawler.Document(
                            current,
                            document.html().getBytes(StandardCharsets.UTF_8),
                            contentType));
        }

        // prevent from being banned for flooding
        handleThrottling(current);

        return true;
    }

    private void handleThrottling(String current) throws InterruptedException {
        int delayMs = getCrawlerDelayFromRobots(current);
        if (configuration.getMinTimeBetweenRequests() > 0) {
            if (delayMs > 0) {
                delayMs = Math.min(configuration.getMinTimeBetweenRequests(), delayMs);
            } else {
                delayMs = configuration.getMinTimeBetweenRequests();
            }
        }
        // prevent from being banned for flooding
        if (delayMs > 0) {
            Thread.sleep(delayMs);
        }
    }

    private void handleTemporaryError(String current, URLReference reference) {
        int currentCount = status.temporaryErrorOnUrl(current);
        if (currentCount >= configuration.getMaxErrorCount()) {
            log.info("Too many errors ({}) on url {}, skipping it", currentCount, current);
            discardUrl(current, reference);
        } else {
            log.info("Putting back the url {} into the backlog", current);
            forceAddUrl(current, reference.type(), reference.depth());
        }
    }

    private int getCrawlerDelayFromRobots(String current) {
        String domain = getDomainFromUrl(current);
        SimpleRobotRules rules = robotsRules.get(domain);
        if (rules != null) {
            return (int) rules.getCrawlDelay();
        } else {
            return 0;
        }
    }

    static String getDomainFromUrl(String url) {
        int beginDomain = url.indexOf("://");
        if (beginDomain <= 0) {
            return "";
        }
        int endDomain = url.indexOf("/", beginDomain + 3);
        if (endDomain <= 0) {
            return url.substring(beginDomain + 3);
        }
        return url.substring(beginDomain + 3, endDomain);
    }

    private boolean isUrlForbidden(String location) {
        if (!configuration.isAllowedUrl(location)) {
            return true;
        }

        String domain = getDomainFromUrl(location);
        SimpleRobotRules rules = robotsRules.get(domain);
        if (rules == null) {
            return false;
        }
        boolean allowed = rules.isAllowed(location);
        if (!allowed) {
            log.info("Url {} is not allowed by the robots.txt file", location);
        }
        return !allowed;
    }

    private void handleRobotsFile(String url) throws Exception {
        HttpResponse<byte[]> response = downloadUrl(url);
        try {
            String contentType = response.headers().firstValue("content-type").orElse("text/plain");
            byte[] body = response.body();
            SimpleRobotRulesParser parser = new SimpleRobotRulesParser();
            SimpleRobotRules simpleRobotRules =
                    parser.parseContent(
                            url, body, contentType, List.of(configuration.getUserAgent()));

            // the first time we see a Robots file we add the sitemaps to the list of urls to crawl
            if (simpleRobotRules.getSitemaps().isEmpty()) {
                log.info("The robots.txt file doesn't contain any site map");
            } else {
                simpleRobotRules
                        .getSitemaps()
                        .forEach(
                                siteMap -> {
                                    log.info("Adding sitemap : {}", siteMap);
                                    // force add the url (no check on the max number of urls)
                                    forceAddUrl(siteMap, URLReference.Type.SITEMAP, 0);
                                });
            }

            // then we store the file in the status
            status.storeRobotsFile(
                    url, new String(response.body(), StandardCharsets.UTF_8), contentType);

            // and then we start applying the new rules
            applyRobotsRules(url, simpleRobotRules);

        } catch (Exception e) {
            log.warn("Error while parsing the robots.txt file", e);
        }
    }

    private void applyRobotsRules(String url, SimpleRobotRules simpleRobotRules) {
        String domain = getDomainFromUrl(url);
        log.info("Applying robots rules for domain {} - rules {}", domain, simpleRobotRules);
        robotsRules.put(domain, simpleRobotRules);
    }

    private void handleSitemapsFile(String url) throws Exception {

        if (isUrlForbidden(url)) {
            log.info("Sitemap {} is forbidden. Skipping", url);
            return;
        }

        try {
            HttpResponse<byte[]> response = downloadUrl(url);
            SiteMapParser siteMapParser = new SiteMapParser();
            AbstractSiteMap abstractSiteMap =
                    siteMapParser.parseSiteMap(response.body(), new URL(url));
            if (abstractSiteMap instanceof SiteMap siteMap) {

                Collection<SiteMapURL> siteMapUrls = siteMap.getSiteMapUrls();

                siteMapUrls.forEach(
                        siteMapUrl -> {
                            log.info("Adding url from sitemap : {}", siteMapUrl.getUrl());
                            String loc = siteMapUrl.getUrl().toString();
                            if (configuration.isAllowedUrl(loc)) {
                                addPageUrl(loc, null);
                            } else {
                                log.debug("Ignoring not allowed url: {}", loc);
                                discardUrl(loc, new URLReference(loc, URLReference.Type.PAGE, 0));
                            }
                        });
            } else if (abstractSiteMap instanceof SiteMapIndex siteMapIndex) {
                log.info("It is a sitemap index");
                Collection<AbstractSiteMap> sitemaps = siteMapIndex.getSitemaps(true);
                sitemaps.forEach(
                        s -> {
                            log.info("Adding this sitemap : {}", s.getUrl());
                            forceAddUrl(s.getUrl().toString(), URLReference.Type.SITEMAP, 0);
                        });
            } else {
                log.warn("Unknown sitemap type: {}", abstractSiteMap.getClass().getName());
            }
        } catch (crawlercommons.sitemaps.UnknownFormatException e) {
            log.warn("Error while parsing the sitemap file at {}", url, e);
        }
    }

    private HttpResponse<byte[]> downloadUrl(String url) throws IOException, InterruptedException {
        HttpClient client = HttpClient.newHttpClient();
        IOException lastError = null;
        for (int i = 0; i < configuration.getMaxErrorCount(); i++) {
            try {
                return client.send(
                        HttpRequest.newBuilder()
                                .uri(URI.create(url))
                                .header("User-Agent", configuration.getUserAgent())
                                .build(),
                        HttpResponse.BodyHandlers.ofByteArray());
            } catch (IOException err) {
                lastError = err;
                log.warn("Error while downloading url: {}", url, err);
                handleThrottling(url);
            }
        }
        if (lastError != null) {
            throw lastError;
        } else {
            throw new IOException("Unknown error while downloading url: " + url);
        }
    }

    public void restartIndexing(Set<String> seedUrls) {
        status.reset();
        for (String url : seedUrls) {
            crawl(url);
        }
        status.setLastIndexStartTimestamp(System.currentTimeMillis());
        status.setLastIndexEndTimestamp(0);
    }

    public void reloadStatus(StatusStorage statusStorage) throws Exception {
        status.reloadFrom(statusStorage);

        // while reloading we need only to rebuild in memory the robots rules
        // these rules have been already parsed with success the first time
        status.getRobotsFiles()
                .forEach(
                        (url, robotsFile) -> {
                            try {
                                SimpleRobotRulesParser parser = new SimpleRobotRulesParser();
                                SimpleRobotRules simpleRobotRules =
                                        parser.parseContent(
                                                url,
                                                robotsFile
                                                        .content()
                                                        .getBytes(StandardCharsets.UTF_8),
                                                robotsFile.contentType(),
                                                List.of(configuration.getUserAgent()));
                                applyRobotsRules(url, simpleRobotRules);
                            } catch (Exception e) {
                                log.warn("Error while parsing the robots.txt file", e);
                            }
                        });
    }
}
