package com.datastax.oss.sga.api.codestorage;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;


/**
 * A code archive is always a zip file that contains the code to be executed by the function agent.
 * It can contain any number of files and directories.
 * There are some special directories:
 * - python: contains the python code
 * - connectors: contains the jars for Kafka Connect connectors
 */
public interface DownloadedCodeArchive {

    /**
     * This method is used to extract the contents of the archive to a directory.
     * The directory must exist.
     * It is expected that the archive is unzipped on the flight to the directory,
     * without creating a temporary file.
     * @param directory
     * @throws CodeStorageException
     * @throws IOException
     */
    void extractTo(Path directory) throws CodeStorageException, IOException;
}
