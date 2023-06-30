package com.datastax.oss.sga.cli.commands;

import com.datastax.oss.sga.cli.SgaCLIConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import lombok.SneakyThrows;
import picocli.CommandLine;

public abstract class BaseCmd implements Runnable {


    protected final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
    @CommandLine.ParentCommand
    protected RootCmd rootCmd;

    private HttpClient httpClient;
    private SgaCLIConfig config;


    @SneakyThrows
    protected SgaCLIConfig getConfig() {
        if (config == null) {
            File configFile;
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
        }
        return config;
    }

    protected String getBaseWebServiceUrl() {
        return getConfig().getWebServiceUrl();
    }

    protected synchronized HttpClient getHttpClient() {
        if (httpClient == null) {
            httpClient = HttpClient.newBuilder()
                    .build();
        }
        return httpClient;
    }

    @SneakyThrows
    protected HttpResponse<String> http(HttpRequest httpRequest) {
        return http(httpRequest, HttpResponse.BodyHandlers.ofString());
    }

    @SneakyThrows
    protected <T> HttpResponse<T> http(HttpRequest httpRequest, HttpResponse.BodyHandler<T> bodyHandler) {
        final HttpResponse<T> response = getHttpClient().send(httpRequest, bodyHandler);
        final int status = response.statusCode();
        if (status >= 200 && status < 300) {
            return response;
        }
        if (status >= 400) {
            log("Request failed: " + response.statusCode());
            log(response.body());
            throw new RuntimeException("Request failed");
        }
        throw new RuntimeException("Unexpected status code: " + status);
    }

    protected HttpRequest newGet(String uri) {
        return HttpRequest.newBuilder()
                        .uri(URI.create("%s/api%s".formatted(getBaseWebServiceUrl(), uri)))
                .GET()
                .build();
    }

    protected HttpRequest newDelete(String uri) {
        return HttpRequest.newBuilder()
                .uri(URI.create("%s/api%s".formatted(getBaseWebServiceUrl(), uri)))
                .DELETE()
                .build();
    }

    protected HttpRequest newPut(String uri, String contentType, HttpRequest.BodyPublisher bodyPublisher) {
        return HttpRequest.newBuilder()
                .uri(URI.create("%s/api%s".formatted(getBaseWebServiceUrl(), uri)))
                .header("Content-Type", contentType)
                .PUT(bodyPublisher)
                .build();
    }


    protected void log(Object log) {
        System.out.println(log);
    }

    protected void debug(Object log) {
        if (rootCmd.isVerbose()) {
            log(log);
        }
    }
}
