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
