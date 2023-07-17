package com.datastax.oss.sga.impl.codestorage;

import com.datastax.oss.sga.api.codestorage.CodeStorageException;

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
