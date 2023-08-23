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
package ai.langstream.runtime.agent.nar;

import ai.langstream.api.codestorage.GenericZipFileArchiveFile;
import ai.langstream.api.codestorage.LocalZipFileArchiveFile;
import com.datastax.dse.driver.api.core.cql.reactive.ReactiveSession;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.zip.ZipUtil;
import org.apache.pulsar.common.nar.FileUtils;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j

public class NarFileHandler implements AutoCloseable {

    private final Path packagesDirectory;
    private final Path temporaryDirectory;

    @Getter
    private List<URLClassLoader> classloaders;
    private Map<String, PackageMetadata> packages = new HashMap<>();

    public NarFileHandler(Path packagesDirectory) throws Exception {
        this.packagesDirectory = packagesDirectory;
        this.temporaryDirectory = Files.createTempDirectory("nar");
    }


    private static void deleteDirectory(Path dir) throws Exception {
        try (DirectoryStream<Path> all = Files.newDirectoryStream(dir)) {
            for (Path file : all) {
                if (Files.isDirectory(file)) {
                    deleteDirectory(file);
                } else {
                    Files.delete(file);
                }
            }
            Files.delete(dir);
        }
    }

    public void close() {

        if (classloaders != null) {
            for (URLClassLoader classLoader : classloaders) {
                try {
                    classLoader.close();
                } catch (Exception err) {
                    log.error("Cannot close classloader {}", classLoader, err);
                }
            }
        }

        try {
            deleteDirectory(temporaryDirectory);
        } catch ( Exception e ) {
            log.error("Cannot delete temporary directory {}", temporaryDirectory, e);
        }
    }

    record PackageMetadata (Path nar, String name, Set<String> agents, Path directory) {}

    public void scan() throws Exception {
        try (DirectoryStream<Path> all = Files.newDirectoryStream(packagesDirectory, "*.nar")) {
            for (Path narFile : all) {
                handleNarFile(narFile);
            }
        } catch (Exception err) {
            log.error("Failed to scan packages directory", err);
            throw err;
        }
    }

    public void handleNarFile(Path narFile) throws Exception {
        String filename = narFile.getFileName().toString();
        Path dest = temporaryDirectory.resolve(narFile.getFileName().toString() + ".dir");
        log.info("Unpacking NAR file {} to {}", narFile, dest);
        GenericZipFileArchiveFile file = new LocalZipFileArchiveFile(narFile);
        file.extractTo(dest);


        Path services = dest.resolve("META-INF/services");
        if (Files.isDirectory(services)) {
            Path agents = services.resolve("ai.langstream.api.runner.code.AgentCodeProvider");
            if (Files.isRegularFile(agents)) {
                PackageMetadata metadata = new PackageMetadata(narFile, filename, Set.of(), dest);
                packages.put(filename, metadata);
            }
        }
    }



    public void createClassloaders(ClassLoader parent) throws Exception {
        classloaders = new ArrayList<>();
        for (PackageMetadata metadata : packages.values()) {
            log.info("Creating classloader for package {}", metadata.name);
            List<URL> urls = new ArrayList<>();

            log.info("Adding agents code {}", metadata.directory);
            urls.add(metadata.directory.toFile().toURI().toURL());

            Path metaInfDirectory = metadata.directory
                    .resolve("META-INF");
            if (Files.isDirectory(metaInfDirectory)) {

                Path dependencies = metaInfDirectory.resolve("bundled-dependencies");
                if (Files.isDirectory(dependencies)) {
                    try (DirectoryStream<Path> allFiles = Files.newDirectoryStream(dependencies);) {
                        for (Path file : allFiles) {
                            if (file.getFileName().toString().endsWith(".jar")) {
                                log.info("Adding dependency {}", file);
                                urls.add(file.toUri().toURL());
                            }
                        }
                    }
                }
            }

            URLClassLoader result = new URLClassLoader(urls.toArray(URL[]::new), parent);
            classloaders.add(result);
        }
    }

}
