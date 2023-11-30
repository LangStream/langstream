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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.notFound;
import static com.github.tomakehurst.wiremock.client.WireMock.okForContentType;
import static com.github.tomakehurst.wiremock.client.WireMock.serviceUnavailable;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.temporaryRedirect;
import static org.junit.jupiter.api.Assertions.*;

import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

@WireMockTest
class WebCrawlerTest {

    @Test
    void testWebSiteErrors(WireMockRuntimeInfo vmRuntimeInfo) throws Exception {

        stubFor(
                get("/index.html")
                        .willReturn(
                                okForContentType(
                                        "text/html",
                                        """
                                  <a href="internalErrorPage.html">link</a>
                                  <a href="notFoundPage.html">link</a>
                              """)));
        stubFor(get("/internalErrorPage.html").willReturn(serviceUnavailable()));
        stubFor(get("/notFoundPage.html").willReturn(notFound()));

        WebCrawlerConfiguration configuration =
                WebCrawlerConfiguration.builder()
                        .allowedDomains(Set.of(vmRuntimeInfo.getHttpBaseUrl()))
                        .handleRobotsFile(false)
                        .maxErrorCount(5)
                        .build();
        WebCrawlerStatus status = new WebCrawlerStatus();
        List<Document> documents = new ArrayList<>();
        WebCrawler crawler = new WebCrawler(configuration, status, documents::add);
        crawler.crawl(vmRuntimeInfo.getHttpBaseUrl() + "/index.html");
        crawler.runCycle();

        assertEquals(1, documents.size());
        assertEquals(vmRuntimeInfo.getHttpBaseUrl() + "/index.html", documents.get(0).url());
        assertEquals(2, status.getPendingUrls().size());
        assertEquals(3, status.getUrls().size());

        // process the internalErrorPage
        crawler.runCycle();

        assertEquals(2, status.getPendingUrls().size());
        assertEquals(3, status.getUrls().size());

        // process the notFoundPage
        crawler.runCycle();

        assertEquals(1, status.getPendingUrls().size());
        assertEquals(3, status.getUrls().size());

        // process the internalErrorPage
        crawler.runCycle();

        assertEquals(1, status.getPendingUrls().size());
        assertEquals(3, status.getUrls().size());

        // now the error page starts to work again
        stubFor(
                get("/internalErrorPage.html")
                        .willReturn(
                                okForContentType(
                                        "text/html",
                                        """
                                  ok !
                              """)));

        // process the internalErrorPage
        crawler.runCycle();

        assertEquals(0, status.getPendingUrls().size());
        assertEquals(3, status.getUrls().size());
    }

    @Test
    void testWebSitePermanentErrors(WireMockRuntimeInfo vmRuntimeInfo) throws Exception {

        stubFor(
                get("/index.html")
                        .willReturn(
                                okForContentType(
                                        "text/html",
                                        """
                                  <a href="internalErrorPage.html">link</a>
                              """)));
        stubFor(get("/internalErrorPage.html").willReturn(serviceUnavailable()));

        // after 3 errors we give up
        WebCrawlerConfiguration configuration =
                WebCrawlerConfiguration.builder()
                        .allowedDomains(Set.of(vmRuntimeInfo.getHttpBaseUrl()))
                        .handleRobotsFile(false)
                        .maxErrorCount(3)
                        .build();
        WebCrawlerStatus status = new WebCrawlerStatus();
        List<Document> documents = new ArrayList<>();
        WebCrawler crawler = new WebCrawler(configuration, status, documents::add);
        crawler.crawl(vmRuntimeInfo.getHttpBaseUrl() + "/index.html");
        crawler.runCycle();

        assertEquals(1, documents.size());
        assertEquals(vmRuntimeInfo.getHttpBaseUrl() + "/index.html", documents.get(0).url());
        assertEquals(1, status.getPendingUrls().size());
        assertEquals(2, status.getUrls().size());

        // process the internalErrorPage
        crawler.runCycle();

        assertEquals(1, status.getPendingUrls().size());
        assertEquals(2, status.getUrls().size());

        // process the internalErrorPage
        crawler.runCycle();

        assertEquals(1, status.getPendingUrls().size());
        assertEquals(2, status.getUrls().size());

        // process the internalErrorPage
        crawler.runCycle();

        assertEquals(0, status.getPendingUrls().size());
        assertEquals(2, status.getUrls().size());

        // nothing to do
        assertFalse(crawler.runCycle());
    }

    @Test
    void testRedirects(WireMockRuntimeInfo vmRuntimeInfo) throws Exception {

        stubFor(
                get("/index.html")
                        .willReturn(
                                okForContentType(
                                        "text/html",
                                        """
                                  <a href="redirectToGoodWebsite.html">link</a>
                                  <a href="redirectToBadWebsite.html">link</a>
                              """)));
        stubFor(
                get("/redirectToGoodWebsite.html")
                        .willReturn(
                                temporaryRedirect(
                                        vmRuntimeInfo.getHttpBaseUrl() + "/goodWebsite.html")));

        stubFor(
                get("/redirectToBadWebsite.html")
                        .willReturn(temporaryRedirect("http://go-away-from-here/somewhere.html")));

        stubFor(get("/goodWebsite.html").willReturn(okForContentType("text/html", "ok")));

        WebCrawlerConfiguration configuration =
                WebCrawlerConfiguration.builder()
                        .allowedDomains(Set.of(vmRuntimeInfo.getHttpBaseUrl()))
                        .handleRobotsFile(false)
                        .build();
        WebCrawlerStatus status = new WebCrawlerStatus();
        List<Document> documents = new ArrayList<>();
        WebCrawler crawler = new WebCrawler(configuration, status, documents::add);
        crawler.crawl(vmRuntimeInfo.getHttpBaseUrl() + "/index.html");
        crawler.runCycle();

        assertEquals(1, documents.size());
        assertEquals(vmRuntimeInfo.getHttpBaseUrl() + "/index.html", documents.get(0).url());
        assertEquals(2, status.getPendingUrls().size());
        assertEquals(3, status.getUrls().size());

        // redirectToGoodWebsite
        crawler.runCycle();
        assertEquals(2, status.getPendingUrls().size());
        assertEquals(4, status.getUrls().size());

        assertEquals(vmRuntimeInfo.getHttpBaseUrl() + "/index.html", documents.get(0).url());
        // the document that did the redirection is not reported to the DocumentVisitor
        assertEquals(1, documents.size());

        // redirectToBadWebsite
        crawler.runCycle();

        assertEquals(1, status.getPendingUrls().size());
        assertEquals(4, status.getUrls().size());

        // goodWebsite
        crawler.runCycle();
        assertEquals(0, status.getPendingUrls().size());
        assertEquals(4, status.getUrls().size());

        assertEquals(vmRuntimeInfo.getHttpBaseUrl() + "/index.html", documents.get(0).url());
        assertEquals(vmRuntimeInfo.getHttpBaseUrl() + "/goodWebsite.html", documents.get(1).url());

        // nothing to do
        assertFalse(crawler.runCycle());
    }

    @Test
    void testNetworkErrors(WireMockRuntimeInfo vmRuntimeInfo) throws Exception {

        stubFor(
                get("/index.html")
                        .willReturn(
                                okForContentType(
                                        "text/html",
                                        """
                                  <a href="internalErrorPage.html">link</a>
                              """)));
        stubFor(
                get("/internalErrorPage.html")
                        .willReturn(aResponse().withFault(Fault.CONNECTION_RESET_BY_PEER)));

        WebCrawlerConfiguration configuration =
                WebCrawlerConfiguration.builder()
                        .allowedDomains(Set.of(vmRuntimeInfo.getHttpBaseUrl()))
                        .handleRobotsFile(false)
                        .maxErrorCount(5)
                        .build();
        WebCrawlerStatus status = new WebCrawlerStatus();
        List<Document> documents = new ArrayList<>();
        WebCrawler crawler = new WebCrawler(configuration, status, documents::add);
        crawler.crawl(vmRuntimeInfo.getHttpBaseUrl() + "/index.html");
        crawler.runCycle();

        assertEquals(1, documents.size());
        assertEquals(vmRuntimeInfo.getHttpBaseUrl() + "/index.html", documents.get(0).url());
        assertEquals(1, status.getPendingUrls().size());
        assertEquals(2, status.getUrls().size());

        // process the internalErrorPage
        crawler.runCycle();

        assertEquals(1, status.getPendingUrls().size());
        assertEquals(2, status.getUrls().size());

        // process the internalErrorPage
        crawler.runCycle();

        assertEquals(1, status.getPendingUrls().size());
        assertEquals(2, status.getUrls().size());

        // now the error page starts to work again
        stubFor(
                get("/internalErrorPage.html")
                        .willReturn(
                                okForContentType(
                                        "text/html",
                                        """
                                  ok !
                              """)));

        // process the internalErrorPage
        crawler.runCycle();

        assertEquals(0, status.getPendingUrls().size());
        assertEquals(2, status.getUrls().size());
    }

    @Test
    void testNetworkErrorsEventuallyFail(WireMockRuntimeInfo vmRuntimeInfo) throws Exception {

        stubFor(
                get("/index.html")
                        .willReturn(
                                okForContentType(
                                        "text/html",
                                        """
                                  <a href="internalErrorPage.html">link</a>
                              """)));
        stubFor(
                get("/internalErrorPage.html")
                        .willReturn(aResponse().withFault(Fault.CONNECTION_RESET_BY_PEER)));

        WebCrawlerConfiguration configuration =
                WebCrawlerConfiguration.builder()
                        .allowedDomains(Set.of(vmRuntimeInfo.getHttpBaseUrl()))
                        .handleRobotsFile(false)
                        .maxErrorCount(1)
                        .build();
        WebCrawlerStatus status = new WebCrawlerStatus();
        List<Document> documents = new ArrayList<>();
        WebCrawler crawler = new WebCrawler(configuration, status, documents::add);
        crawler.crawl(vmRuntimeInfo.getHttpBaseUrl() + "/index.html");
        crawler.runCycle();

        assertEquals(1, documents.size());
        assertEquals(vmRuntimeInfo.getHttpBaseUrl() + "/index.html", documents.get(0).url());
        assertEquals(1, status.getPendingUrls().size());
        assertEquals(2, status.getUrls().size());

        // process the internalErrorPage
        crawler.runCycle();

        // we gave up, too many errors
        assertEquals(0, status.getPendingUrls().size());
        assertEquals(2, status.getUrls().size());
    }

    @Test
    void testBinaryContent(WireMockRuntimeInfo vmRuntimeInfo) throws Exception {

        byte[] mockPdf = new byte[] {1, 2, 3, 4, 5};
        stubFor(
                get("/index.html")
                        .willReturn(
                                okForContentType(
                                        "text/html",
                                        """
                                  <a href="document.pdf">link</a>
                              """)));
        stubFor(
                get("/document.pdf")
                        .willReturn(
                                aResponse()
                                        .withHeader("content-type", "application/pdf")
                                        .withBody(mockPdf)));

        WebCrawlerConfiguration configuration =
                WebCrawlerConfiguration.builder()
                        .allowedDomains(Set.of(vmRuntimeInfo.getHttpBaseUrl()))
                        .allowNonHtmlContents(true)
                        .handleRobotsFile(false)
                        .maxErrorCount(5)
                        .build();
        WebCrawlerStatus status = new WebCrawlerStatus();
        List<Document> documents = new ArrayList<>();
        WebCrawler crawler = new WebCrawler(configuration, status, documents::add);
        crawler.crawl(vmRuntimeInfo.getHttpBaseUrl() + "/index.html");
        crawler.runCycle();

        assertEquals(1, documents.size());
        assertEquals(vmRuntimeInfo.getHttpBaseUrl() + "/index.html", documents.get(0).url());
        assertEquals(1, status.getPendingUrls().size());
        assertEquals(2, status.getUrls().size());

        crawler.runCycle();

        assertEquals(vmRuntimeInfo.getHttpBaseUrl() + "/document.pdf", documents.get(1).url());
        assertArrayEquals(mockPdf, documents.get(1).content());
        assertEquals("application/pdf", documents.get(1).contentType());

        assertEquals(0, status.getPendingUrls().size());
        assertEquals(2, status.getUrls().size());
    }
}
