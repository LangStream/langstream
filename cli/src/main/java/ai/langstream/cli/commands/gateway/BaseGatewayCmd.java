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
package ai.langstream.cli.commands.gateway;

import ai.langstream.cli.commands.BaseCmd;
import ai.langstream.cli.commands.RootCmd;
import ai.langstream.cli.commands.RootGatewayCmd;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URLEncoder;
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
        String paramsPart = "";
        String optionsPart = "";
        if (userParams != null) {
            paramsPart = userParams.entrySet()
                    .stream()
                    .map(e -> encodeParam(e, "param:"))
                    .collect(Collectors.joining("&"));

        }

        if (options != null) {
            optionsPart = options.entrySet()
                    .stream()
                    .map(e -> encodeParam(e, "option:"))
                    .collect(Collectors.joining("&"));
        }

        String credentialsPart = "";
        if (credentials != null) {
            credentialsPart = encodeParam("credentials", credentials, "");
        }

        return List.of(credentialsPart, paramsPart, optionsPart)
                .stream().collect(Collectors.joining("&"));
    }



    private String encodeParam(Map.Entry<String, String> e, String prefix) {
        return encodeParam(e.getKey(), e.getValue(), prefix);
    }

    @SneakyThrows
    private String encodeParam(String key, String value, String prefix) {
        return "%s=%s".formatted(prefix + key, URLEncoder.encode(value, "UTF-8"));
    }


}
