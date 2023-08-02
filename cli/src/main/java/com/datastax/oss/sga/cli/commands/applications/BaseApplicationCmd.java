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

import com.datastax.oss.sga.cli.commands.BaseCmd;
import com.datastax.oss.sga.cli.commands.RootAppCmd;
import com.datastax.oss.sga.cli.commands.RootCmd;
import picocli.CommandLine;

public abstract class BaseApplicationCmd extends BaseCmd {

    @CommandLine.ParentCommand
    private RootAppCmd rootAppCmd;


    @Override
    protected RootCmd getRootCmd() {
        return rootAppCmd.getRootCmd();
    }


    protected String tenantAppPath(String uri) {
        final String tenant = getConfig().getTenant();
        if (tenant == null) {
            throw new IllegalStateException("Tenant not set. Run 'sga configure tenant <tenant>' to set it.");
        }
        debug("Using tenant: %s".formatted(tenant));
        return "/applications/%s%s".formatted(tenant, uri);
    }
}
