/**
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
package com.datastax.oss.sga.api.codestorage;

import com.datastax.oss.sga.api.codestorage.CodeStorageException;
import com.datastax.oss.sga.api.codestorage.GenericZipFileArchiveFile;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * This is local zip file archive file.
 */
public class LocalZipFileArchiveFile extends GenericZipFileArchiveFile {

    private Path path;

    public LocalZipFileArchiveFile(Path path) {
        this.path = path;
    }

    @Override
    public InputStream getData() throws IOException, CodeStorageException {
        return Files.newInputStream(path);
    }

}
