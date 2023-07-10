package com.datastax.oss.sga.cli.commands.tenants;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import lombok.SneakyThrows;
import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.ZipParameters;
import picocli.CommandLine;

@CommandLine.Command(name = "put",
        description = "Put a tenant")
public class PutTenantCmd extends BaseTenantCmd {

    @CommandLine.Parameters(description = "Name of the tenant")
    private String name;

    @Override
    @SneakyThrows
    public void run() {
        http(newPut(pathForTenant(name),
                "application/json",
                HttpRequest.BodyPublishers.ofString("{}")));
        log("tenant %s created/updated".formatted(name));

    }
}
