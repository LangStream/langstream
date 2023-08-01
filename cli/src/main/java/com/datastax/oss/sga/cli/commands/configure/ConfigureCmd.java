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
package com.datastax.oss.sga.cli.commands.configure;

import com.datastax.oss.sga.cli.commands.BaseCmd;
import com.datastax.oss.sga.cli.commands.RootCmd;
import lombok.Getter;
import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(name = "configure", mixinStandardHelpOptions = true, description = "Configure SGA tenant and authentication")
@Getter
public class ConfigureCmd extends BaseCmd {

    public enum ConfigKey {
        webServiceUrl,
        apiGatewayUrl,
        tenant,
        token;
    }

    @CommandLine.ParentCommand
    private RootCmd rootCmd;

    @CommandLine.Parameters(description = "Config key to configure")
    private ConfigKey configKey;

    @CommandLine.Parameters(description = "Value to set")
    private String newValue;


    @Override
    @SneakyThrows
    public void run() {
        updateConfig(clientConfig -> {
            switch (configKey) {
                case tenant:
                    clientConfig.setTenant(newValue);
                    break;
                case webServiceUrl:
                    clientConfig.setWebServiceUrl(newValue);
                    break;
                case apiGatewayUrl:
                    clientConfig.setApiGatewayUrl(newValue);
                    break;
                case token:
                    clientConfig.setToken(newValue);
                    break;
            }
        });
        log("Config updated: " + configKey + "=" + newValue);
    }
}
