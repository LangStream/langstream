package com.datastax.oss.sga.impl.codestorage;

import com.datastax.oss.sga.api.codestorage.CodeStorageException;
import com.datastax.oss.sga.api.codestorage.UploadableCodeArchive;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class LocalFileUploadableCodeArchive implements UploadableCodeArchive {
    private final Path zipFile;

    public LocalFileUploadableCodeArchive(Path zipFile) {
        this.zipFile = zipFile;
    }

    @Override
    public InputStream getData() throws IOException, CodeStorageException {
        return Files.newInputStream(zipFile);
    }
}
