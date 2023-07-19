package com.datastax.oss.sga.cli.commands;

import com.datastax.oss.sga.cli.SgaCLIConfig;
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
import lombok.SneakyThrows;
import picocli.CommandLine;

public abstract class BaseCmd implements Runnable {


    protected final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

    @CommandLine.Spec
    protected CommandLine.Model.CommandSpec command;
    protected abstract RootCmd getRootCmd();

    private HttpClient httpClient;
    private SgaCLIConfig config;


    @SneakyThrows
    protected SgaCLIConfig getConfig() {
        if (config == null) {
            File configFile;
            final RootCmd rootCmd = getRootCmd();
            if (rootCmd.getConfigPath() == null) {
                String configBaseDir = System.getProperty("basedir");
                if (configBaseDir == null) {
                    configBaseDir = System.getProperty("user.dir");
                }
                configFile = Path.of(configBaseDir, "conf", "cli.yaml").toFile();
            } else {
                configFile = new File(rootCmd.getConfigPath());
            }
            if (!configFile.exists()) {
                throw new IllegalStateException("Config file not found: " + configFile);
            }
            config = objectMapper.readValue(configFile, SgaCLIConfig.class);
            overrideFromEnv(config);
        }
        return config;
    }

    @SneakyThrows
    protected void updateConfig(Consumer<SgaCLIConfig> consumer) {
        consumer.accept(getConfig());
        File configFile = computeConfigFile();
        Files.write(configFile.toPath(), objectMapper.writeValueAsBytes(config));
    }

    private File computeConfigFile() {
        File configFile;
        final RootCmd rootCmd = getRootCmd();
        if (rootCmd.getConfigPath() == null) {
            String configBaseDir = System.getProperty("basedir");
            if (configBaseDir == null) {
                configBaseDir = System.getProperty("user.dir");
            }
            configFile = Path.of(configBaseDir, "conf", "cli.yaml").toFile();
        } else {
            configFile = new File(rootCmd.getConfigPath());
        }
        return configFile;
    }


    @SneakyThrows
    private void overrideFromEnv(SgaCLIConfig config) {
        for (Field field : SgaCLIConfig.class.getDeclaredFields()) {
            final String name = field.getName();
            final String newValue = System.getenv("SGA_" + name);
            if (newValue != null) {
                log("Using env variable: %s=%s".formatted(name, newValue));
                field.trySetAccessible();
                field.set(config, newValue);
            }
        }
    }

    protected String getBaseWebServiceUrl() {
        return getConfig().getWebServiceUrl();
    }

    protected synchronized HttpClient getHttpClient() {
        if (httpClient == null) {
            httpClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(30))
                    .build();
        }
        return httpClient;
    }

    @SneakyThrows
    protected HttpResponse<String> http(HttpRequest httpRequest) {
        return http(httpRequest, HttpResponse.BodyHandlers.ofString());
    }

    protected <T> HttpResponse<T> http(HttpRequest httpRequest, HttpResponse.BodyHandler<T> bodyHandler) {
        try {
            final HttpResponse<T> response = getHttpClient().send(httpRequest, bodyHandler);
            final int status = response.statusCode();
            if (status >= 200 && status < 300) {
                return response;
            }
            if (status >= 400) {
                err("Request failed: " + response.statusCode());
                err(response.body());
                throw new RuntimeException();
            }
            throw new RuntimeException("Unexpected status code: " + status);
        } catch (ConnectException error) {
            throw new RuntimeException("Cannot connect to " + getBaseWebServiceUrl(), error);
        } catch (IOException | InterruptedException error) {
            throw new RuntimeException("Unexpected network error " + error, error);
        }
    }

    protected HttpRequest newGet(String uri) {
        return HttpRequest.newBuilder()
                        .uri(URI.create("%s/api%s".formatted(getBaseWebServiceUrl(), uri)))
                .version(HttpClient.Version.HTTP_1_1)
                .GET()
                .build();
    }

    protected HttpRequest newDependencyGet(URL uri) throws URISyntaxException  {
        return HttpRequest.newBuilder()
                .uri(uri.toURI())
                .version(HttpClient.Version.HTTP_1_1)
                .GET()
                .build();
    }

    protected HttpRequest newDelete(String uri) {
        return HttpRequest.newBuilder()
                .uri(URI.create("%s/api%s".formatted(getBaseWebServiceUrl(), uri)))
                .version(HttpClient.Version.HTTP_1_1)
                .DELETE()
                .build();
    }

    protected HttpRequest newPut(String uri, String contentType, HttpRequest.BodyPublisher bodyPublisher) {
        return HttpRequest.newBuilder()
                .uri(URI.create("%s/api%s".formatted(getBaseWebServiceUrl(), uri)))
                .header("Content-Type", contentType)
                .version(HttpClient.Version.HTTP_1_1)
                .PUT(bodyPublisher)
                .build();
    }


    protected void log(Object log) {
        command.commandLine().getOut().println(log);
    }

    protected void err(Object log) {
        System.err.println(command.commandLine().getColorScheme().errorText(log.toString()));
    }

    protected void debug(Object log) {
        if (getRootCmd().isVerbose()) {
            log(log);
        }
    }
}
