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
package ai.langstream.cli.util;

import ai.langstream.cli.commands.VersionProvider;
import lombok.AllArgsConstructor;
import lombok.Getter;

public class DockerImageUtils {

    public static DockerImage computeDockerImage(
            String dockerImageVersion, String dockerImageName) {
        if (dockerImageVersion == null) {
            dockerImageVersion = VersionProvider.getMavenVersion();
            if (dockerImageVersion != null && dockerImageVersion.endsWith("-SNAPSHOT")) {
                // built-from-sources, not a release
                dockerImageVersion = "latest-dev";
            }
        }

        if (dockerImageName == null) {
            if (dockerImageVersion != null && dockerImageVersion.equals("latest-dev")) {
                // built-from-sources, not a release
                dockerImageName = "langstream/langstream-runtime-tester";
            } else {
                // default to latest
                dockerImageName = "ghcr.io/langstream/langstream-runtime-tester";
            }
        }

        return new DockerImage(dockerImageName, dockerImageVersion);
    }

    @AllArgsConstructor
    @Getter
    public static class DockerImage {
        final String name;
        final String version;

        public String getFullName() {
            return name + ":" + version;
        }
    }
}
