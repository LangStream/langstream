package ai.langstream.cli.commands;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import picocli.CommandLine;

public class VersionProvider implements CommandLine.IVersionProvider {

    private static final String[] VERSION = new String[] {getVersionFromJar()};

    public String[] getVersion() {
        return VERSION;
    }

    private static String getVersionFromJar() {
        try {
            Manifest manifest = getManifest();
            final String version = getVersionFromManifest(manifest);
            final String gitRevision = getGitRevisionFromManifest(manifest);
            return "LangStream CLI %s (%s)".formatted(version, gitRevision);
        } catch (Throwable t) {
            // never ever let this exception bubble up otherwise any command will fail
            return "Error: %s".formatted(t.getMessage());
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
