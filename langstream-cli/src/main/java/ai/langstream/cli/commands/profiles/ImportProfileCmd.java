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

import ai.langstream.cli.NamedProfile;
import ai.langstream.cli.Profile;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Base64;
import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(name = "import", header = "Import profile from file or inline json")
public class ImportProfileCmd extends BaseProfileCmd {

    @CommandLine.Parameters(description = "Name of the profile")
    private String name;

    @CommandLine.Option(
            names = {"-f", "--file"},
            description = "Import profile from file")
    private String fromFile;

    @CommandLine.Option(
            names = {"-i", "--inline"},
            description = "Import profile from inline json")
    private String inline;

    @CommandLine.Option(
            names = {"-u", "--update"},
            description =
                    "Allow updating the profile if it already exists. Note that the configuration will be overwritten and NOT merged")
    private boolean allowUpdate;

    @CommandLine.Option(
            names = {"--set-current"},
            description = "Set this profile as current")
    private boolean setAsCurrent;

    @Override
    @SneakyThrows
    public void run() {
        checkGlobalFlags();

        if (fromFile == null && inline == null) {
            throw new IllegalArgumentException("Either --file or --inline must be specified");
        }
        if (fromFile != null && inline != null) {
            throw new IllegalArgumentException("Only one of --file or --inline must be specified");
        }

        final Profile profile;

        if (fromFile != null) {

            final File file = new File(fromFile);
            if (!Files.isRegularFile(file.toPath())) {
                throw new IllegalArgumentException(
                        String.format("File %s does not exist", fromFile));
            }
            profile = yamlConfigReader.readValue(new File(fromFile), Profile.class);
        } else if (inline != null) {
            String jsonProfile;
            if (inline.startsWith("base64:")) {
                final String base64Value = inline.substring("base64:".length());
                jsonProfile =
                        new String(Base64.getDecoder().decode(base64Value), StandardCharsets.UTF_8);
            } else {
                jsonProfile = inline;
            }
            profile = jsonConfigReader.readValue(jsonProfile, Profile.class);
        } else {
            throw new IllegalStateException();
        }

        final NamedProfile newProfile = new NamedProfile();
        newProfile.setName(name);
        newProfile.setTenant(profile.getTenant());
        newProfile.setApiGatewayUrl(profile.getApiGatewayUrl());
        newProfile.setWebServiceUrl(profile.getWebServiceUrl());
        newProfile.setToken(profile.getToken());

        validateProfile(newProfile);
        final NamedProfile existing = getProfile(name);
        if (!allowUpdate && existing != null) {
            throw new IllegalArgumentException(String.format("Profile %s already exists", name));
        }

        updateConfig(
                langStreamCLIConfig -> {
                    langStreamCLIConfig.updateProfile(name, newProfile);
                    if (existing == null) {
                        log(String.format("profile %s created", name));
                    } else {
                        log(String.format("profile %s updated", name));
                    }

                    if (setAsCurrent) {
                        langStreamCLIConfig.setCurrentProfile(name);
                        log(String.format("profile %s set as current", name));
                    }
                });
    }
}
