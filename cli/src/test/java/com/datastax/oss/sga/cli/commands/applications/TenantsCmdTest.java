package com.datastax.oss.sga.cli.commands.applications;

import static com.github.tomakehurst.wiremock.client.WireMock.aMultipart;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.BinaryEqualToPattern;
import com.github.tomakehurst.wiremock.matching.MatchResult;
import com.github.tomakehurst.wiremock.matching.MultipartValuePatternBuilder;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


class TenantsCmdTest extends CommandTestBase {


    @Test
    public void testPut() throws Exception {
        wireMock.register(WireMock.put("/api/tenants/%s".formatted("newt")).willReturn(WireMock.ok("{ \"name\": \"newt\" }")));
        CommandResult result = executeCommand("tenants", "put", "newt");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals("tenant newt created/updated", result.out());

    }


    @Test
    public void testGet() throws Exception {
        wireMock.register(WireMock.get("/api/tenants/%s".formatted("newt")).willReturn(WireMock.ok("{ \"name\": \"newt\" }")));
        CommandResult result = executeCommand("tenants", "get", "newt");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals("{ \"name\": \"newt\" }", result.out());

    }

    @Test
    public void testDelete() throws Exception {
        wireMock.register(WireMock.delete("/api/tenants/%s"
                .formatted("newt")).willReturn(WireMock.ok()));

        CommandResult result = executeCommand("tenants", "delete", "newt");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals("Tenant newt deleted", result.out());

    }

    @Test
    public void testList() throws Exception {
        wireMock.register(WireMock.get("/api/tenants"
                .formatted(TENANT)).willReturn(WireMock.ok("{}")));

        CommandResult result = executeCommand("tenants", "list");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals("{}", result.out());

    }
}