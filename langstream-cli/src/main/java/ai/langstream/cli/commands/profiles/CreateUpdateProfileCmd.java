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
import lombok.SneakyThrows;
import picocli.CommandLine;

public abstract class CreateUpdateProfileCmd extends BaseProfileCmd {

    @CommandLine.Parameters(description = "Name of the profile")
    private String name;

    @CommandLine.Option(
            names = {"--set-current"},
            description = "Set this profile as current")
    private boolean setAsCurrent;

    @CommandLine.Option(
            names = {"--web-service-url"},
            description = "webServiceUrl of the profile")
    private String webServiceUrl;

    @CommandLine.Option(
            names = {"--api-gateway-url"},
            description = "apiGatewayUrl of the profile")
    private String apiGatewayUrl;

    @CommandLine.Option(
            names = {"--tenant"},
            description = "tenant of the profile")
    private String tenant;

    @CommandLine.Option(
            names = {"--token"},
            description = "token of the profile")
    private String token;

    @Override
    @SneakyThrows
    public void run() {
        checkGlobalFlags();

        NamedProfile profile = getProfile(name);
        if (isCreate()) {
            if (profile != null) {
                throw new IllegalArgumentException(
                        String.format("Profile %s already exists", name));
            }
            profile = new NamedProfile();
            profile.setName(name);
        } else {
            if (profile == null) {
                throw new IllegalArgumentException(getProfileNotFoundMessage(name));
            }
        }

        if (webServiceUrl != null) {
            profile.setWebServiceUrl(webServiceUrl);
        }
        if (apiGatewayUrl != null) {
            profile.setApiGatewayUrl(apiGatewayUrl);
        }
        if (tenant != null) {
            profile.setTenant(tenant);
        }
        if (token != null) {
            profile.setToken(token);
        }
        validateProfile(profile);

        final NamedProfile finalProfile = profile;

        updateConfig(
                langStreamCLIConfig -> {
                    langStreamCLIConfig.updateProfile(name, finalProfile);
                    if (isCreate()) {
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

    protected abstract boolean isCreate();

    @CommandLine.Command(name = "create", header = "Create a new profile")
    public static class CreateProfileCmd extends CreateUpdateProfileCmd {

        @Override
        protected boolean isCreate() {
            return true;
        }
    }

    @CommandLine.Command(name = "update", header = "Update an existing profile")
    public static class UpdateProfileCmd extends CreateUpdateProfileCmd {

        @Override
        protected boolean isCreate() {
            return false;
        }
    }
}
