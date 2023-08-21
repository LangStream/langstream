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
package ai.langstream.cli.client;

import ai.langstream.cli.LangStreamCLIConfig;
import ai.langstream.cli.commands.RootCmd;
import ai.langstream.cli.util.MultiPartBodyPublisher;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.ConnectException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.function.Consumer;
import lombok.Builder;
import lombok.SneakyThrows;


public class LangStreamClient {
    private static final ObjectMapper yamlConfigReader = new ObjectMapper(new YAMLFactory());
    private final String configPath;
    private final Logger logger;
    private LangStreamCLIConfig config;
    private HttpClient httpClient;

    public interface Logger {
        void log(Object message);
        void error(Object message);
        void debug(Object message);
    }


    public interface Applications {
        void deploy(String application, MultiPartBodyPublisher multiPartBodyPublisher);
        void update(String application, MultiPartBodyPublisher multiPartBodyPublisher);
        void delete(String application);
        String get(String application);
        String list();
    }

    public LangStreamClient(String configPath, Logger logger) {
        this.configPath = configPath;
        this.logger = logger;
        loadConfig();
    }

    @SneakyThrows
    private void loadConfig() {
        if (config == null) {
            File configFile = computeConfigFile();
            config = yamlConfigReader.readValue(configFile, LangStreamCLIConfig.class);
            overrideFromEnv(config);
        }
    }


    @SneakyThrows
    public LangStreamCLIConfig getConfig() {
        return config;
    }

    @SneakyThrows
    public void updateConfig(Consumer<LangStreamCLIConfig> consumer) {
        consumer.accept(getConfig());
        File configFile = computeConfigFile();
        Files.write(configFile.toPath(), yamlConfigReader.writeValueAsBytes(config));
    }

    private File computeConfigFile() {
        File configFile;
        if (configPath == null) {
            String configBaseDir = System.getProperty("basedir");
            if (configBaseDir == null) {
                configBaseDir = System.getProperty("user.dir");
            }
            configFile = Path.of(configBaseDir, "conf", "cli.yaml").toFile();
        } else {
            configFile = new File(configPath);
        }
        return configFile;
    }


    @SneakyThrows
    private void overrideFromEnv(LangStreamCLIConfig config) {
        for (Field field : LangStreamCLIConfig.class.getDeclaredFields()) {
            final String name = field.getName();
            final String newValue = System.getenv("LANGSTREAM_" + name);
            if (newValue != null) {
                logger.log("Using env variable: %s=%s".formatted(name, newValue));
                field.trySetAccessible();
                field.set(config, newValue);
            }
        }
    }

    protected String getBaseWebServiceUrl() {
        return getConfig().getWebServiceUrl();
    }

    public synchronized HttpClient getHttpClient() {
        if (httpClient == null) {
            httpClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(30))
                    .followRedirects(HttpClient.Redirect.ALWAYS)
                    .build();
        }
        return httpClient;
    }

    @SneakyThrows
    public HttpResponse<String> http(HttpRequest httpRequest) {
        return http(httpRequest, HttpResponse.BodyHandlers.ofString());
    }

    public <T> HttpResponse<T> http(HttpRequest httpRequest, HttpResponse.BodyHandler<T> bodyHandler) {
        try {
            final HttpResponse<T> response = getHttpClient().send(httpRequest, bodyHandler);
            final int status = response.statusCode();
            if (status >= 200 && status < 300) {
                return response;
            }
            if (status >= 400) {
                final T body = response.body();
                if (body != null) {
                    logger.error(body);
                }
                throw new RuntimeException("Request failed: " + response.statusCode());
            }
            throw new RuntimeException("Unexpected status code: " + status);
        } catch (ConnectException error) {
            throw new RuntimeException("Cannot connect to " + httpRequest.uri(), error);
        } catch (IOException | InterruptedException error) {
            throw new RuntimeException("Unexpected network error " + error, error);
        }
    }

    private HttpRequest.Builder withAuth(HttpRequest.Builder builder) {
        final String token = getConfig().getToken();
        if (token == null) {
            return builder;
        }
        return builder.header("Authorization", "Bearer " + token);
    }

    public HttpRequest newGet(String uri) {
        return withAuth(
                HttpRequest.newBuilder()
                        .uri(URI.create("%s/api%s".formatted(getBaseWebServiceUrl(), uri)))
                        .version(HttpClient.Version.HTTP_1_1)
                        .GET()
        )
                .build();
    }

    public HttpRequest newDependencyGet(URL uri) throws URISyntaxException {
        return withAuth(HttpRequest.newBuilder()
                .uri(uri.toURI())
                .version(HttpClient.Version.HTTP_1_1)
                .GET()
        )
                .build();
    }

    public HttpRequest newDelete(String uri) {
        return withAuth(HttpRequest.newBuilder()
                .uri(URI.create("%s/api%s".formatted(getBaseWebServiceUrl(), uri)))
                .version(HttpClient.Version.HTTP_1_1)
                .DELETE()
        )
                .build();
    }

    public HttpRequest newPut(String uri, String contentType, HttpRequest.BodyPublisher bodyPublisher) {
        return withAuth(HttpRequest.newBuilder()
                .uri(URI.create("%s/api%s".formatted(getBaseWebServiceUrl(), uri)))
                .header("Content-Type", contentType)
                .version(HttpClient.Version.HTTP_1_1)
                .PUT(bodyPublisher)
        )
                .build();
    }

    public HttpRequest newPatch(String uri, String contentType, HttpRequest.BodyPublisher bodyPublisher) {
        return withAuth(HttpRequest.newBuilder()
                .uri(URI.create("%s/api%s".formatted(getBaseWebServiceUrl(), uri)))
                .header("Content-Type", contentType)
                .version(HttpClient.Version.HTTP_1_1)
                .method("PATCH", bodyPublisher)
        )
                .build();
    }


    public HttpRequest newPost(String uri, String contentType, HttpRequest.BodyPublisher bodyPublisher) {
        return withAuth(HttpRequest.newBuilder()
                .uri(URI.create("%s/api%s".formatted(getBaseWebServiceUrl(), uri)))
                .header("Content-Type", contentType)
                .version(HttpClient.Version.HTTP_1_1)
                .POST(bodyPublisher)
        )
                .build();
    }

    public String tenantAppPath(String uri) {
        final String tenant = getConfig().getTenant();
        if (tenant == null) {
            throw new IllegalStateException("Tenant not set. Run 'langstream configure tenant <tenant>' to set it.");
        }
        logger.debug("Using tenant: %s".formatted(tenant));
        return "/applications/%s%s".formatted(tenant, uri);
    }



    public Applications applications() {
        return new Applications() {
            @Override
            public void deploy(String application, MultiPartBodyPublisher multiPartBodyPublisher) {
                final String path = tenantAppPath("/" + application);
                final String contentType = "multipart/form-data; boundary=%s".formatted(multiPartBodyPublisher.getBoundary());
                final HttpRequest request = newPost(path, contentType, multiPartBodyPublisher.build());
                http(request);
            }

            @Override
            public void update(String application, MultiPartBodyPublisher multiPartBodyPublisher) {
                final String path = tenantAppPath("/" + application);
                final String contentType = "multipart/form-data; boundary=%s".formatted(multiPartBodyPublisher.getBoundary());
                final HttpRequest request = newPatch(path, contentType, multiPartBodyPublisher.build());
                http(request);
            }

            @Override
            public void delete(String application) {
                http(newDelete(tenantAppPath("/" + application)));
            }

            @Override
            public String get(String application) {
                return http(newGet(tenantAppPath("/" + application))).body();
            }

            @Override
            public String list() {
                return http(newGet(tenantAppPath(""))).body();
            }
        };
    }




}
