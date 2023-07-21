package com.datastax.oss.sga.cli.commands.configure;

import com.datastax.oss.sga.cli.commands.BaseCmd;
import com.datastax.oss.sga.cli.commands.RootCmd;
import lombok.Getter;
import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(name = "configure", mixinStandardHelpOptions = true, description = "Configure SGA tenant and authentication")
@Getter
public class ConfigureCmd extends BaseCmd {

    @CommandLine.ParentCommand
    private RootCmd rootCmd;


    @CommandLine.Parameters(description = "Tenant to use")
    private String tenant;


    @Override
    @SneakyThrows
    public void run() {
        updateConfig(config -> config.setTenant(tenant));
        log("Config updated, now using tenant: " + tenant);
    }
}
