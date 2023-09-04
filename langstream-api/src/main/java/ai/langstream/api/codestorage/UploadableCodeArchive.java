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

/**
 * A code archive is always a zip file that contains the code to be executed by the function agent.
 * It can contain any number of files and directories. There are some special directories: - python:
 * contains the python code - connectors: contains the jars for Kafka Connect connectors
 */
public interface UploadableCodeArchive {

    /**
     * This method is used for raw file upload.
     *
     * @return the input stream to read the data from
     */
    InputStream getData() throws IOException, CodeStorageException;

    /**
     * Sha256 hex digest of the application Python binaries data.
     *
     * @return
     */
    String getPyBinariesDigest();

    /**
     * Sha256 hex digest of the application Java binaries data.
     *
     * @return
     */
    String getJavaBinariesDigest();
}
