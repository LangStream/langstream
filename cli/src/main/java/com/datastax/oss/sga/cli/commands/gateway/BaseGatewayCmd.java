package com.datastax.oss.sga.cli.commands.gateway;

import com.datastax.oss.sga.cli.commands.BaseCmd;
import com.datastax.oss.sga.cli.commands.RootCmd;
import com.datastax.oss.sga.cli.commands.RootGatewayCmd;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URLEncoder;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import picocli.CommandLine;

public abstract class BaseGatewayCmd extends BaseCmd {

    protected static final ObjectMapper messageMapper = new ObjectMapper();

    @CommandLine.ParentCommand
    private RootGatewayCmd cmd;

    @Override
    protected RootCmd getRootCmd() {
        return cmd.getRootCmd();
    }


    protected String computeQueryString(Map<String, String> queryStringParams, String prefix) {
        if (queryStringParams == null || queryStringParams.isEmpty()) {
            return "";
        }
        return queryStringParams.entrySet()
                .stream()
                .map(e -> encodeParam(e, prefix))
                .collect(Collectors.joining("&"));

    }


    @SneakyThrows
    private String encodeParam(Map.Entry<String, String> e, String prefix) {
        return "%s=%s".formatted(prefix + e.getKey(), URLEncoder.encode(e.getValue(), "UTF-8"));
    }


}
