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
package ai.langstream.admin.client;

import ai.langstream.admin.client.http.HttpClientFacade;
import ai.langstream.admin.client.http.HttpClientProperties;
import ai.langstream.admin.client.http.Retry;
import ai.langstream.admin.client.model.Applications;
import ai.langstream.admin.client.model.Archetypes;
import ai.langstream.admin.client.util.MultiPartBodyPublisher;
import ai.langstream.admin.client.util.Slf4jLAdminClientLogger;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AdminClient implements AutoCloseable {

    private final AdminClientConfiguration configuration;
    private final AdminClientLogger logger;
    private final HttpClientFacade httpClientFacade;

    private static final ObjectMapper mapper = new ObjectMapper();

    public AdminClient(AdminClientConfiguration adminClientConfiguration) {
        this(adminClientConfiguration, new Slf4jLAdminClientLogger(log));
    }

    public AdminClient(
            AdminClientConfiguration adminClientConfiguration, AdminClientLogger logger) {
        this(adminClientConfiguration, logger, new HttpClientProperties());
    }

    public AdminClient(
            AdminClientConfiguration adminClientConfiguration,
            AdminClientLogger logger,
            HttpClientProperties httpClientProperties) {
        this.configuration = adminClientConfiguration;
        this.logger = logger;
        this.httpClientFacade = new HttpClientFacade(logger, httpClientProperties);
    }

    protected String getBaseWebServiceUrl() {
        return configuration.getWebServiceUrl();
    }

    public HttpClientFacade getHttpClientFacade() {
        return httpClientFacade;
    }

    public HttpClient getHttpClient() {
        return httpClientFacade.getHttpClient();
    }

    public HttpResponse<String> http(HttpRequest httpRequest) throws HttpRequestFailedException {
        return httpClientFacade.http(httpRequest);
    }

    public <T> HttpResponse<T> http(
            HttpRequest httpRequest, HttpResponse.BodyHandler<T> bodyHandler)
            throws HttpRequestFailedException {
        return httpClientFacade.http(httpRequest, bodyHandler);
    }

    public <T> HttpResponse<T> http(
            HttpRequest httpRequest, HttpResponse.BodyHandler<T> bodyHandler, Retry retry)
            throws HttpRequestFailedException {
        return httpClientFacade.http(httpRequest, bodyHandler, retry);
    }

    private HttpRequest.Builder withAuth(HttpRequest.Builder builder) {
        final String token = configuration.getToken();
        if (token == null) {
            return builder;
        }
        return builder.header("Authorization", "Bearer " + token);
    }

    public HttpRequest newGet(String uri) {
        return withAuth(
                        HttpRequest.newBuilder()
                                .uri(
                                        URI.create(
                                                String.format(
                                                        "%s/api%s", getBaseWebServiceUrl(), uri)))
                                .version(HttpClient.Version.HTTP_1_1)
                                .GET())
                .build();
    }

    public HttpRequest newDependencyGet(URL uri) throws URISyntaxException {
        return withAuth(
                        HttpRequest.newBuilder()
                                .uri(uri.toURI())
                                .version(HttpClient.Version.HTTP_1_1)
                                .GET())
                .build();
    }

    public HttpRequest newDelete(String uri) {
        return withAuth(
                        HttpRequest.newBuilder()
                                .uri(
                                        URI.create(
                                                String.format(
                                                        "%s/api%s", getBaseWebServiceUrl(), uri)))
                                .version(HttpClient.Version.HTTP_1_1)
                                .DELETE())
                .build();
    }

    public HttpRequest newPut(
            String uri, String contentType, HttpRequest.BodyPublisher bodyPublisher) {
        return withAuth(
                        HttpRequest.newBuilder()
                                .uri(
                                        URI.create(
                                                String.format(
                                                        "%s/api%s", getBaseWebServiceUrl(), uri)))
                                .header("Content-Type", contentType)
                                .version(HttpClient.Version.HTTP_1_1)
                                .PUT(bodyPublisher))
                .build();
    }

    public HttpRequest newPatch(
            String uri, String contentType, HttpRequest.BodyPublisher bodyPublisher) {
        return withAuth(
                        HttpRequest.newBuilder()
                                .uri(
                                        URI.create(
                                                String.format(
                                                        "%s/api%s", getBaseWebServiceUrl(), uri)))
                                .header("Content-Type", contentType)
                                .version(HttpClient.Version.HTTP_1_1)
                                .method("PATCH", bodyPublisher))
                .build();
    }

    public HttpRequest newPost(
            String uri, String contentType, HttpRequest.BodyPublisher bodyPublisher) {
        return withAuth(
                        HttpRequest.newBuilder()
                                .uri(
                                        URI.create(
                                                String.format(
                                                        "%s/api%s", getBaseWebServiceUrl(), uri)))
                                .header("Content-Type", contentType)
                                .version(HttpClient.Version.HTTP_1_1)
                                .POST(bodyPublisher))
                .build();
    }

    public String tenantAppPath(String uri) {
        final String tenant = configuration.getTenant();
        if (tenant == null) {
            throw new IllegalStateException(
                    "Tenant not set. Please set the tenant in the configuration.");
        }
        logger.debug(String.format("Using tenant: %s", tenant));
        return String.format("/applications/%s%s", tenant, uri);
    }

    public String tenantArchetypesPath(String uri) {
        final String tenant = configuration.getTenant();
        if (tenant == null) {
            throw new IllegalStateException(
                    "Tenant not set. Please set the tenant in the configuration.");
        }
        logger.debug(String.format("Using tenant: %s", tenant));
        return String.format("/archetypes/%s%s", tenant, uri);
    }

    public String tenantArchetypePath(String archetypeId, String uri) {
        return tenantArchetypesPath(String.format("/%s%s", archetypeId, uri));
    }

    static String formatQueryString(Map<String, String> params) {
        if (params == null || params.isEmpty()) {
            return "";
        }
        return "?"
                + params.entrySet().stream()
                        .filter(e -> e.getValue() != null && !e.getValue().isBlank())
                        .map(
                                e ->
                                        String.format(
                                                "%s=%s",
                                                URLEncoder.encode(
                                                        e.getKey(), StandardCharsets.UTF_8),
                                                URLEncoder.encode(
                                                        e.getValue(), StandardCharsets.UTF_8)))
                        .collect(Collectors.joining("&"));
    }

    public Applications applications() {
        return new ApplicationsImpl();
    }

    public Archetypes archetypes() {
        return new ArchetypesImpl();
    }

    private class ArchetypesImpl implements Archetypes {
        @Override
        @SneakyThrows
        public String list() {
            return http(newGet(tenantArchetypesPath(""))).body();
        }

        @Override
        @SneakyThrows
        public String get(String archetype) {
            return http(newGet(tenantArchetypesPath("/" + archetype))).body();
        }
    }

    private class ApplicationsImpl implements Applications {
        @Override
        public String deploy(String application, MultiPartBodyPublisher multiPartBodyPublisher) {
            return deploy(application, multiPartBodyPublisher, false, false);
        }

        @Override
        @SneakyThrows
        public String deploy(
                String application,
                MultiPartBodyPublisher multiPartBodyPublisher,
                boolean dryRun,
                boolean autoUpgrade) {
            final String path =
                    tenantAppPath("/" + application)
                            + "?dry-run="
                            + dryRun
                            + "&auto-upgrade="
                            + autoUpgrade;
            final String contentType =
                    String.format(
                            "multipart/form-data; boundary=%s",
                            multiPartBodyPublisher.getBoundary());
            final HttpRequest request = newPost(path, contentType, multiPartBodyPublisher.build());
            return http(request).body();
        }

        @Override
        @SneakyThrows
        public String deployFromArchetype(
                String applicationId,
                String archetypeId,
                Map<String, Object> parameters,
                boolean dryRun) {
            // /api/archetypes/my-tenant/simple/applications/app-id
            final String path =
                    tenantArchetypePath(archetypeId, "/applications/" + applicationId)
                            + "?dry-run="
                            + dryRun;
            final String contentType = "application/json";
            final String parametersJson = mapper.writeValueAsString(parameters);
            final HttpRequest request =
                    newPost(path, contentType, HttpRequest.BodyPublishers.ofString(parametersJson));
            return http(request).body();
        }

        @Override
        @SneakyThrows
        public void update(
                String application,
                MultiPartBodyPublisher multiPartBodyPublisher,
                boolean autoUpgrade,
                boolean forceRestart) {
            final String path =
                    tenantAppPath("/" + application)
                            + "?auto-upgrade="
                            + autoUpgrade
                            + "&force-restart="
                            + forceRestart;
            final String contentType =
                    String.format(
                            "multipart/form-data; boundary=%s",
                            multiPartBodyPublisher.getBoundary());
            final HttpRequest request = newPatch(path, contentType, multiPartBodyPublisher.build());
            http(request);
        }

        @Override
        @SneakyThrows
        public void delete(String application, boolean force) {
            http(newDelete(tenantAppPath("/" + application + "?force=" + force)));
        }

        @Override
        @SneakyThrows
        public String get(String application, boolean stats) {
            return http(newGet(tenantAppPath("/" + application + "?stats=" + stats))).body();
        }

        @Override
        @SneakyThrows
        public String list() {
            return http(newGet(tenantAppPath(""))).body();
        }

        @Override
        public HttpResponse<byte[]> download(String application) {
            return download(application, (String) null);
        }

        @Override
        public HttpResponse<byte[]> download(String application, String codeStorageId) {
            return download(application, null, HttpResponse.BodyHandlers.ofByteArray());
        }

        @Override
        public <T> HttpResponse<T> download(
                String application, HttpResponse.BodyHandler<T> responseBodyHandler) {
            return download(application, null, responseBodyHandler);
        }

        @Override
        @SneakyThrows
        public <T> HttpResponse<T> download(
                String application,
                String codeArchiveId,
                HttpResponse.BodyHandler<T> responseBodyHandler) {

            final String uri =
                    codeArchiveId == null
                            ? tenantAppPath("/" + application + "/code")
                            : tenantAppPath("/" + application + "/code/" + codeArchiveId);
            return http(newGet(uri), responseBodyHandler);
        }

        @Override
        @SneakyThrows
        public String getCodeInfo(String application, String codeArchiveId) {
            final String path =
                    tenantAppPath("/" + application + "/code/" + codeArchiveId + "/info");
            return http(newGet(path)).body();
        }

        @Override
        @SneakyThrows
        public HttpResponse<InputStream> logs(
                String application, List<String> filter, String format) {
            final String filterStr = filter == null ? "" : String.join(",", filter);
            final String query =
                    formatQueryString(
                            Map.of("filter", filterStr, "format", format == null ? "" : format));
            final HttpRequest request = newGet(tenantAppPath("/" + application + "/logs" + query));
            return http(request, HttpResponse.BodyHandlers.ofInputStream());
        }
    }

    @Override
    public void close() {
        httpClientFacade.close();
    }
}
