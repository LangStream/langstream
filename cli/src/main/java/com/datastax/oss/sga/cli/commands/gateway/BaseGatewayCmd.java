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
package com.datastax.oss.sga.cli.commands.gateway;

import com.datastax.oss.sga.cli.commands.BaseCmd;
import com.datastax.oss.sga.cli.commands.RootCmd;
import com.datastax.oss.sga.cli.commands.RootGatewayCmd;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
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


    protected String computeQueryString(String credentials, Map<String, String> userParams, Map<String, String> options) {
        Map<String, String> all = new HashMap<>();
        if (userParams != null) {
            userParams.entrySet()
                    .stream()
                    .forEach(e -> all.put("param:" + e.getKey(), e.getValue()));

        }

        if (options != null) {
            options.entrySet()
                    .stream()
                    .forEach(e -> all.put("option:" + e.getKey(), e.getValue()));
        }

        if (credentials != null) {
            all.put("credentials", credentials);
        }

        return all.entrySet()
                .stream()
                .map(e -> encodeParam(e))
                .collect(Collectors.joining("&"));
    }



    private String encodeParam(Map.Entry<String, String> e) {
        return encodeParam(e.getKey(), e.getValue());
    }

    @SneakyThrows
    private String encodeParam(String key, String value) {
        return "%s=%s".formatted(key, URLEncoder.encode(value, "UTF-8"));
    }


}
