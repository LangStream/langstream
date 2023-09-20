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
package ai.langstream.cli;

import ai.langstream.cli.commands.profiles.BaseProfileCmd;
import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Map;
import java.util.TreeMap;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class LangStreamCLIConfig extends Profile {

    private Map<String, NamedProfile> profiles = new TreeMap<>();

    private String currentProfile = BaseProfileCmd.DEFAULT_PROFILE_NAME;

    @JsonIgnore
    public void updateProfile(String name, NamedProfile namedProfile) {
        if (name.equals(BaseProfileCmd.DEFAULT_PROFILE_NAME)) {
            setTenant(namedProfile.getTenant());
            setToken(namedProfile.getToken());
            setWebServiceUrl(namedProfile.getWebServiceUrl());
            setApiGatewayUrl(namedProfile.getApiGatewayUrl());
        } else {
            profiles.put(name, namedProfile);
        }
    }
}
