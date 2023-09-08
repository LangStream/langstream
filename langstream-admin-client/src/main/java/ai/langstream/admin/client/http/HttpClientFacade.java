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
package ai.langstream.admin.client.http;

import ai.langstream.admin.client.AdminClientLogger;
import ai.langstream.admin.client.HttpRequestFailedException;
import ai.langstream.admin.client.util.Slf4jLAdminClientLogger;
import java.io.IOException;
import java.net.ConnectException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HttpClientFacade implements AutoCloseable {

    private final AdminClientLogger logger;
    private final HttpClientProperties httpClientProperties;
    private ExecutorService executorService;
    private HttpClient httpClient;

    public HttpClientFacade() {
        this(new HttpClientProperties());
    }

    public HttpClientFacade(HttpClientProperties httpClientProperties) {
        this(new Slf4jLAdminClientLogger(log), httpClientProperties);
    }

    public HttpClientFacade(AdminClientLogger logger, HttpClientProperties httpClientProperties) {
        this.logger = logger;
        this.httpClientProperties = httpClientProperties;
    }

    public synchronized HttpClient getHttpClient() {
        if (httpClient == null) {
            executorService = Executors.newCachedThreadPool();
            httpClient =
                    HttpClient.newBuilder()
                            .executor(executorService)
                            .connectTimeout(Duration.ofSeconds(30))
                            .followRedirects(HttpClient.Redirect.ALWAYS)
                            .build();
        }
        return httpClient;
    }

    public HttpResponse<String> http(HttpRequest httpRequest) throws HttpRequestFailedException {
        return http(httpRequest, HttpResponse.BodyHandlers.ofString());
    }

    public <T> HttpResponse<T> http(
            HttpRequest httpRequest, HttpResponse.BodyHandler<T> bodyHandler)
            throws HttpRequestFailedException {
        return http(httpRequest, bodyHandler, httpClientProperties.getRetry().get());
    }

    public <T> HttpResponse<T> http(
            HttpRequest httpRequest, HttpResponse.BodyHandler<T> bodyHandler, Retry retry)
            throws HttpRequestFailedException {

        try {
            if (logger.isDebugEnabled()) {
                logger.debug("sending request: " + httpRequest);
            }
            final HttpResponse<T> response = getHttpClient().send(httpRequest, bodyHandler);
            if (logger.isDebugEnabled()) {
                logger.debug("received response: " + response);
            }
            if (shouldRetry(httpRequest, response, retry, null)) {
                return http(httpRequest, bodyHandler, retry);
            }
            final int status = response.statusCode();
            if (status >= 200 && status < 300) {
                return response;
            }
            if (status >= 400) {
                throw new HttpRequestFailedException(httpRequest, response);
            }
            throw new RuntimeException("Unexpected status code: " + status);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(error);
        } catch (ConnectException error) {
            if (shouldRetry(httpRequest, null, retry, error)) {
                return http(httpRequest, bodyHandler, retry);
            }
            throw new RuntimeException("Cannot connect to " + httpRequest.uri(), error);
        } catch (IOException error) {
            if (shouldRetry(httpRequest, null, retry, error)) {
                return http(httpRequest, bodyHandler, retry);
            }
            throw new RuntimeException("Unexpected network error " + error, error);
        }
    }

    private <T> boolean shouldRetry(
            HttpRequest httpRequest, HttpResponse response, Retry retry, Exception error) {
        final Optional<Long> next = retry.shouldRetryAfter(error, httpRequest, response);
        if (next.isPresent()) {
            try {
                Thread.sleep(next.get());
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(interruptedException);
            }
            return true;
        }
        return false;
    }

    @Override
    public void close() {
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }
}
