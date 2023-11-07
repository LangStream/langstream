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
package ai.langstream.agents.http;

import ai.langstream.ai.agents.commons.JsonRecord;
import ai.langstream.ai.agents.commons.MutableRecord;
import ai.langstream.api.runner.code.AbstractAgentCode;
import ai.langstream.api.runner.code.AgentProcessor;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.RecordSink;
import ai.langstream.api.runtime.ComponentType;
import ai.langstream.api.util.ConfigurationUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.samskivert.mustache.Mustache;
import com.samskivert.mustache.Template;
import java.net.CookieManager;
import java.net.CookiePolicy;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;

@Slf4j
public class HttpRequestAgent extends AbstractAgentCode implements AgentProcessor {

    static final ObjectMapper mapper = new ObjectMapper();
    private final Map<Schema, Schema> avroValueSchemaCache = new ConcurrentHashMap<>();

    private final Map<Schema, Schema> avroKeySchemaCache = new ConcurrentHashMap<>();

    private ExecutorService executor;
    private HttpClient httpClient;
    private String url;
    private String method;
    private Map<String, Template> queryStringTemplates;
    private Map<String, Template> headersTemplates;
    private Template bodyTemplate;
    private String outputFieldName;

    @SuppressWarnings("unchecked")
    @Override
    public void init(Map<String, Object> configuration) {
        this.url =
                ConfigurationUtils.requiredNonEmptyField(
                        configuration, "url", () -> "http-request agent");
        this.outputFieldName =
                ConfigurationUtils.requiredNonEmptyField(
                        configuration, "output-field", () -> "http-request agent");
        this.method = ConfigurationUtils.getString("method", "GET", configuration);
        final String body = ConfigurationUtils.getString("body", null, configuration);
        final Map<String, Object> headers =
                ConfigurationUtils.getMap("headers", new HashMap<>(), configuration);
        final Map<String, Object> queryString =
                ConfigurationUtils.getMap("query-string", new HashMap<>(), configuration);
        final boolean allowRedirects =
                ConfigurationUtils.getBoolean("allow-redirects", true, configuration);
        final boolean handleCookies =
                ConfigurationUtils.getBoolean("handle-cookies", true, configuration);

        queryStringTemplates = new HashMap<>();
        for (Map.Entry<String, Object> entry : queryString.entrySet()) {
            queryStringTemplates.put(
                    entry.getKey(), Mustache.compiler().compile(entry.getValue().toString()));
        }

        headersTemplates = new HashMap<>();
        for (Map.Entry<String, Object> entry : headers.entrySet()) {
            headersTemplates.put(
                    entry.getKey(), Mustache.compiler().compile(entry.getValue().toString()));
        }
        if (body != null) {
            bodyTemplate = Mustache.compiler().compile(body);
        }

        executor = Executors.newCachedThreadPool();
        CookieManager cookieManager = new CookieManager();
        cookieManager.setCookiePolicy(
                handleCookies ? CookiePolicy.ACCEPT_ALL : CookiePolicy.ACCEPT_NONE);
        httpClient =
                HttpClient.newBuilder()
                        .followRedirects(
                                allowRedirects
                                        ? HttpClient.Redirect.NORMAL
                                        : HttpClient.Redirect.NEVER)
                        .cookieHandler(cookieManager)
                        .executor(executor)
                        .build();
    }

    @Override
    public void process(List<Record> records, RecordSink recordSink) {
        for (Record record : records) {
            processRecord(record, recordSink);
        }
    }

    @Override
    public ComponentType componentType() {
        return ComponentType.PROCESSOR;
    }

    public void processRecord(Record record, RecordSink recordSink) {
        try {
            MutableRecord context = MutableRecord.recordToMutableRecord(record, true);
            final JsonRecord jsonRecord = context.toJsonRecord();

            final URI uri = URI.create(url + computeQueryString(jsonRecord));
            final HttpRequest.BodyPublisher bodyPublisher;
            if (bodyTemplate != null) {
                bodyPublisher =
                        HttpRequest.BodyPublishers.ofString(bodyTemplate.execute(jsonRecord));
            } else {
                bodyPublisher = HttpRequest.BodyPublishers.noBody();
            }
            final HttpRequest.Builder requestBuilder =
                    HttpRequest.newBuilder()
                            .uri(uri)
                            .version(HttpClient.Version.HTTP_1_1)
                            .method(this.method, bodyPublisher);
            headersTemplates.forEach(
                    (key, value) -> requestBuilder.header(key, value.execute(jsonRecord)));
            final HttpRequest request = requestBuilder.build();

            httpClient
                    .sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .thenAccept(
                            response -> {
                                try {
                                    if (response.statusCode() >= 400) {
                                        throw new RuntimeException(
                                                "Error processing record: "
                                                        + record
                                                        + " with response: "
                                                        + response);
                                    }
                                    final Object body = parseResponseBody(response);
                                    context.setResultField(
                                            body,
                                            outputFieldName,
                                            null,
                                            avroKeySchemaCache,
                                            avroValueSchemaCache);
                                    Optional<Record> recordResult =
                                            MutableRecord.mutableRecordToRecord(context);
                                    if (log.isDebugEnabled()) {
                                        log.debug("recordResult {}", recordResult);
                                    }
                                    if (recordResult.isPresent()) {
                                        recordSink.emit(
                                                new SourceRecordAndResult(
                                                        record, List.of(recordResult.get()), null));
                                    } else {
                                        recordSink.emit(
                                                new SourceRecordAndResult(record, List.of(), null));
                                    }
                                } catch (Exception e) {
                                    log.error("Error processing record: {}", record, e);
                                    recordSink.emit(
                                            new SourceRecordAndResult(record, List.of(), e));
                                }
                            })
                    .exceptionally(
                            error -> {
                                log.error("Error processing record: {}", record, error);
                                recordSink.emit(new SourceRecordAndResult(record, null, error));
                                return null;
                            });
        } catch (Throwable error) {
            log.error("Error processing record: {}", record, error);
            recordSink.emit(new SourceRecordAndResult(record, null, error));
        }
    }

    private Object parseResponseBody(HttpResponse<String> response) {
        try {
            return mapper.readValue(response.body(), Map.class);
        } catch (JsonProcessingException ex) {
            log.debug("Not able to parse response to json: {}, {}", response.body(), ex);
        }
        return response.body();
    }

    private String computeQueryString(JsonRecord jsonRecord) {
        if (queryStringTemplates.isEmpty()) {
            return "";
        }

        return "?"
                + queryStringTemplates.entrySet().stream()
                        .map(
                                e -> {
                                    final String resolved = e.getValue().execute(jsonRecord);
                                    return encodeParam(e.getKey(), resolved);
                                })
                        .collect(Collectors.joining("&"));
    }

    private static String encodeParam(String key, String value) {
        return String.format("%s=%s", key, URLEncoder.encode(value, StandardCharsets.UTF_8));
    }

    @Override
    public void close() throws Exception {
        if (executor != null) {
            executor.shutdownNow();
        }
    }
}
