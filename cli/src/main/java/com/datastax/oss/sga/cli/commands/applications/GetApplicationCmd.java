package com.datastax.oss.sga.cli.commands.applications;

import static com.datastax.oss.sga.cli.commands.applications.ListApplicationCmd.COLUMNS_FOR_RAW;
import static com.datastax.oss.sga.cli.commands.applications.ListApplicationCmd.getRawFormatValuesSupplier;
import com.datastax.oss.sga.cli.commands.BaseCmd;
import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(name = "get",
        description = "Get SGA application status")
public class GetApplicationCmd extends BaseApplicationCmd {

    @CommandLine.Parameters(description = "Name of the application")
    private String name;

    @CommandLine.Option(names = {"-o"}, description = "Output format")
    private Formats format = Formats.raw;

    @Override
    @SneakyThrows
    public void run() {
        final String body = http(newGet(tenantAppPath("/" + name))).body();
        print(format, body, COLUMNS_FOR_RAW, getRawFormatValuesSupplier());
    }
}
