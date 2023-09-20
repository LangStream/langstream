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

import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(name = "delete", header = "Delete an existing profile")
public class DeleteProfileCmd extends BaseProfileCmd {

    @CommandLine.Parameters(description = "Name of the profile")
    private String name;

    @Override
    @SneakyThrows
    public void run() {
        checkGlobalFlags();
        if (DEFAULT_PROFILE_NAME.equals(name)) {
            throw new IllegalArgumentException(
                    String.format("Profile name %s can't be deleted", name));
        }
        getProfileOrThrow(name);
        updateConfig(
                langStreamCLIConfig -> {
                    if (name.equals(langStreamCLIConfig.getCurrentProfile())) {
                        throw new IllegalArgumentException("Cannot delete the current profile");
                    }

                    langStreamCLIConfig.getProfiles().remove(name);
                    log(String.format("profile %s deleted", name));
                });
    }
}
