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
package ai.langstream.cli.utils;

import ai.langstream.cli.commands.GitIgnoreParser;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;
import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.model.ZipParameters;

public class ApplicationPackager {

    public static Path buildZip(File appDirectory, Consumer<String> logger) throws IOException {
        final Path tempZip = Files.createTempFile("app", ".zip");
        try (final ZipFile zip = new ZipFile(tempZip.toFile())) {
            addApp(appDirectory, zip, logger);
        }
        return tempZip;
    }

    private static void addApp(File appDirectory, ZipFile zip, Consumer<String> logger)
            throws IOException {
        if (appDirectory == null) {
            return;
        }
        logger.accept(String.format("packaging app: %s", appDirectory.getAbsolutePath()));
        if (appDirectory.isDirectory()) {
            File ignoreFile = appDirectory.toPath().resolve(".langstreamignore").toFile();
            if (ignoreFile.exists()) {
                GitIgnoreParser parser = new GitIgnoreParser(ignoreFile.toPath());
                addDirectoryFilesWithLangstreamIgnore(appDirectory, appDirectory, parser, zip);
            } else {
                for (File file : appDirectory.listFiles()) {
                    if (file.isDirectory()) {
                        zip.addFolder(file);
                    } else {
                        zip.addFile(file);
                    }
                }
            }
        } else {
            zip.addFile(appDirectory);
        }
        logger.accept("app packaged");
    }

    private static void addDirectoryFilesWithLangstreamIgnore(
            File appDirectory, File directory, GitIgnoreParser parser, ZipFile zip)
            throws IOException {
        for (File file : directory.listFiles()) {
            if (!parser.matches(file)) {
                if (file.isDirectory()) {
                    addDirectoryFilesWithLangstreamIgnore(appDirectory, file, parser, zip);
                } else {
                    ZipParameters zipParameters = new ZipParameters();
                    String filename = appDirectory.toURI().relativize(file.toURI()).getPath();
                    zipParameters.setFileNameInZip(filename);
                    zip.addFile(file, zipParameters);
                }
            }
        }
    }
}
