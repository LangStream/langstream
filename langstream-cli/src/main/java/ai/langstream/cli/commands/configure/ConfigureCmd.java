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
package ai.langstream.cli.commands.configure;

import ai.langstream.cli.commands.BaseCmd;
import ai.langstream.cli.commands.RootCmd;
import java.util.Arrays;
import lombok.Getter;
import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(name = "configure", mixinStandardHelpOptions = true, description = "Configure LangStream tenant and authentication")
@Getter
public class ConfigureCmd extends BaseCmd {

    public enum ConfigKey {
        webServiceUrl,
        apiGatewayUrl,
        tenant,
        token
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
                case tenant -> clientConfig.setTenant(newValue);
                case webServiceUrl -> clientConfig.setWebServiceUrl(newValue);
                case apiGatewayUrl -> clientConfig.setApiGatewayUrl(newValue);
                case token -> clientConfig.setToken(newValue);
                default -> throw new IllegalArgumentException(
                    "Unknown config key: %s. Only: %s".formatted(configKey, Arrays.toString(ConfigKey.values())));
            }
        });
        log("Config updated: %s=%s".formatted(configKey, newValue));
    }
}
