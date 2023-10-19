package ai.langstream.cli.util;

import java.io.IOException;
import java.nio.file.Path;

public interface GitClient {

    void cloneRepository(Path directory, String uri, String branch) throws IOException;


    String updateRepository(Path directory, String branch) throws IOException;
}
