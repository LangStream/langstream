package ai.langstream.runtime;

import java.nio.file.Files;
import java.nio.file.Path;

public abstract class RuntimeStarter {


    public abstract void start(String... args) throws Exception;

    protected Path getPathFromEnv(String envVar, String defaultValue) {
        return getPathFromEnv(envVar, defaultValue, true);
    }

    protected Path getOptionalPathFromEnv(String envVar) {
        return getPathFromEnv(envVar, null, false);
    }

    protected Path getOptionalPathFromEnv(String envVar, String defaultValue) {
        return getPathFromEnv(envVar, defaultValue, false);
    }

    private Path getPathFromEnv(String envVar, String defaultValue, boolean required) {
        String value = getEnv(envVar);
        if (value == null) {
            value = defaultValue;
        }
        if (!required && value == null) {
            return null;
        }
        final Path path = Path.of(value);
        if (!Files.exists(path)) {
            if (required) {
                throw new IllegalArgumentException("File " + path + " does not exist");
            } else {
                return null;
            }
        }

        return path;
    }

    protected String getEnv(String key) {
        return System.getenv(key);
    }
}
