/*
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
package ai.langstream.cli.commands.profiles;

import ai.langstream.cli.LangStreamCLIConfig;
import ai.langstream.cli.NamedProfile;
import ai.langstream.cli.Profile;
import ai.langstream.cli.commands.BaseCmd;
import ai.langstream.cli.commands.RootCmd;
import ai.langstream.cli.commands.RootProfileCmd;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import picocli.CommandLine;

public abstract class BaseProfileCmd extends BaseCmd {

    public static final String DEFAULT_PROFILE_NAME = "default";
    @CommandLine.ParentCommand private RootProfileCmd cmd;

    @Override
    protected RootCmd getRootCmd() {
        return cmd.getRootCmd();
    }

    protected void checkGlobalFlags() {
        if (getRootCmd().getProfile() != null) {
            throw new IllegalArgumentException(
                    "Global profile flag is not allowed for profiles commands");
        }
    }

    protected void validateProfile(Profile profile) {
        if (profile.getWebServiceUrl() == null || profile.getWebServiceUrl().isBlank()) {
            throw new IllegalArgumentException("webServiceUrl is required");
        }
    }

    protected List<NamedProfile> listAllProfiles() {
        final LangStreamCLIConfig config = getConfig();
        List<NamedProfile> all = new ArrayList<>();
        final NamedProfile defaultProfile = getDefaultProfile();
        all.add(defaultProfile);
        final Collection<NamedProfile> values = config.getProfiles().values();
        all.addAll(values);
        return all;
    }

    protected NamedProfile getProfile(String name) {
        if (DEFAULT_PROFILE_NAME.equals(name)) {
            return getDefaultProfile();
        }
        final NamedProfile profile = getConfig().getProfiles().get(name);
        return profile;
    }

    protected NamedProfile getProfileOrThrow(String name) {
        final NamedProfile profile = getProfile(name);
        if (profile == null) {
            throw new IllegalArgumentException(getProfileNotFoundMessage(name));
        }
        return profile;
    }

    protected String getProfileNotFoundMessage(String name) {
        return String.format(
                "Profile %s not found, maybe you meant one of these: %s",
                name,
                listAllProfiles().stream()
                        .map(NamedProfile::getName)
                        .collect(Collectors.joining(", ")));
    }
}
