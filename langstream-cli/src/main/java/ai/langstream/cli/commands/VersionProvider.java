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
package ai.langstream.cli.commands;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import picocli.CommandLine;

public class VersionProvider implements CommandLine.IVersionProvider {

    private static List<String> VERSION_INFO = null;

    public static void initialize(String label, String forceVersion) {
        VERSION_INFO = getVersionFromJar(label, forceVersion);
    }

    public static String getMavenVersion() {
        return VERSION_INFO.get(1);
    }

    public String[] getVersion() {
        return new String[] {VERSION_INFO.get(0)};
    }

    private static List<String> getVersionFromJar(String label, String forceVersion) {
        try {
            final String version;
            final String gitRevision;
            if (forceVersion != null) {
                version = forceVersion;
                gitRevision = "";
            } else {
                final Manifest manifest = getManifest();
                version = getVersionFromManifest(manifest);
                gitRevision = " (" + getGitRevisionFromManifest(manifest) + ")";
            }
            return List.of(String.format("%s %s", label, version + gitRevision), version);
        } catch (Throwable t) {
            // never ever let this exception bubble up otherwise any command will fail
            return List.of(String.format("Error: %s", t.getMessage()), "unknown");
        }
    }

    private static String getVersionFromManifest(Manifest manifest) {
        final String implVersion = findAttributeByKey(manifest, "Implementation-Version");
        if (implVersion == null) {
            throw new RuntimeException(
                    "Failed to read version from manifest: Implementation-Version not found in META-INF/MANIFEST.MF "
                            + "for langstream-cli jar.");
        }
        return implVersion;
    }

    private static String getGitRevisionFromManifest(Manifest manifest) {
        final String implVersion = findAttributeByKey(manifest, "Implementation-Git-Revision");
        if (implVersion == null) {
            throw new RuntimeException(
                    "Failed to read version from manifest: Implementation-Git-Revision not found in META-INF/MANIFEST.MF "
                            + "for langstream-cli jar.");
        }
        return implVersion;
    }

    private static Manifest getManifest() throws IOException {
        Enumeration<URL> resources =
                VersionProvider.class.getClassLoader().getResources("META-INF/MANIFEST.MF");
        while (resources.hasMoreElements()) {
            try (final InputStream in = resources.nextElement().openStream(); ) {
                Manifest manifest = new Manifest(in);
                final String implTitle = findAttributeByKey(manifest, "Implementation-Title");
                if (implTitle == null) {
                    continue;
                }
                if ("langstream-cli".equals(implTitle)) {
                    return manifest;
                }
            } catch (IOException ioException) {
            }
        }
        throw new RuntimeException(
                "Failed to read version from manifest: META-INF/MANIFEST.MF not found with Implementation-Title: "
                        + "langstream-cli.");
    }

    private static String findAttributeByKey(Manifest manifest, String key) {
        final Attributes mainAttributes = manifest.getMainAttributes();

        final Object implTitle =
                mainAttributes.entrySet().stream()
                        .filter(e -> e.getKey().toString().equals(key))
                        .map(e -> e.getValue())
                        .findFirst()
                        .orElse(null);
        if (implTitle == null) {
            return null;
        }
        return implTitle.toString();
    }
}
