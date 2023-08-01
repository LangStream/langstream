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
