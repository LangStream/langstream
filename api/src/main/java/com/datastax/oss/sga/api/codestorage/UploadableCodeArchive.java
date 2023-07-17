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
public interface UploadableCodeArchive {

    /**
     * This method is used for raw file upload.
     * @return
     */
    InputStream getData() throws IOException, CodeStorageException;
}
