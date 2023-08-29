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
package ai.langstream.api.codestorage;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

/**
 * A code archive is always a zip file that contains the code to be executed by the function agent.
 * It can contain any number of files and directories. There are some special directories: - python:
 * contains the python code - connectors: contains the jars for Kafka Connect connectors
 */
public interface DownloadedCodeArchive {

    InputStream getInputStream() throws IOException, CodeStorageException;

    byte[] getData() throws IOException, CodeStorageException;

    /**
     * This method is used to extract the contents of the archive to a directory. The directory must
     * exist. It is expected that the archive is unzipped on the flight to the directory, without
     * creating a temporary file.
     *
     * @param directory the directory to extract to
     * @throws CodeStorageException if the archive cannot be extracted
     */
    void extractTo(Path directory) throws CodeStorageException;
}
