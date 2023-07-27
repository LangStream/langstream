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
