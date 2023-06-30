package com.datastax.oss.sga.cli.commands;

import com.datastax.oss.sga.cli.SgaCLIConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.net.http.HttpClient;
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

    protected void log(Object log) {
        System.out.println(log);
    }

    protected void debug(Object log) {
        if (rootCmd.isVerbose()) {
            log(log);
        }
    }
}
