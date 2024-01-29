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
package ai.langstream.impl.nar;

import ai.langstream.api.codestorage.GenericZipFileArchiveFile;
import ai.langstream.api.codestorage.LocalZipFileArchiveFile;
import ai.langstream.api.runner.assets.AssetManagerRegistry;
import ai.langstream.api.runner.code.AgentCodeRegistry;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeRegistry;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NarFileHandler
        implements AutoCloseable,
                AgentCodeRegistry.AgentPackageLoader,
                AssetManagerRegistry.AssetManagerPackageLoader,
                TopicConnectionsRuntimeRegistry.TopicConnectionsPackageLoader {

    private static final boolean CLOSE_CLASSLOADERS =
            Boolean.parseBoolean(System.getProperty("langstream.nar.closeClassloaders", "true"));

    static {
        log.info("langstream.nar.closeClassloaders = {}", CLOSE_CLASSLOADERS);
    }

    private final Path packagesDirectory;
    private final Path temporaryDirectory;

    private final List<URL> customLibClasspath;
    private final ClassLoader parentClassloader;

    private List<URLClassLoader> classloaders;
    private final Map<String, PackageMetadata> packages = new HashMap<>();

    public NarFileHandler(
            Path packagesDirectory, List<URL> customLibClasspath, ClassLoader parentClassloader)
            throws Exception {
        this.packagesDirectory = packagesDirectory;
        this.temporaryDirectory = Files.createTempDirectory("nar");
        this.customLibClasspath = customLibClasspath;
        this.parentClassloader = parentClassloader;
    }

    private static void deleteDirectory(Path dir) throws Exception {
        if (!dir.toFile().exists()) {
            return;
        }
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

        // when you are in a docker container or a pod, the JVM is going to be killed
        // no need to close the classloaders
        // closing the classloaders would cause only a log of spammy ClassNotFoundErrors
        // that because false positives reported by users
        if (!CLOSE_CLASSLOADERS) {
            log.info(
                    "Not closing classloaders, "
                            + "in order to avoid class loading issues during development while the JVM is shutdown");
            return;
        }

        for (PackageMetadata metadata : packages.values()) {
            URLClassLoader classLoader = metadata.getClassLoader();
            if (classLoader != null) {
                try {
                    log.info("Closing classloader {}", classLoader);
                    classLoader.close();
                } catch (Exception err) {
                    log.error("Cannot close classloader {}", classLoader, err);
                }
            }
        }

        try {
            deleteDirectory(temporaryDirectory);
        } catch (Exception e) {
            log.error("Cannot delete temporary directory {}", temporaryDirectory, e);
        }
    }

    private static class NarFileClassLoader extends URLClassLoader {

        private final String name;

        public NarFileClassLoader(String name, List<URL> urls, ClassLoader parent) {
            super(urls.toArray(URL[]::new), parent);
            this.name = name;
        }

        @Override
        public String toString() {
            return "NarFileClassLoader{" + "name='" + name + '\'' + '}';
        }

        @Override
        protected Class<?> findClass(String name) throws ClassNotFoundException {
            try {
                return super.findClass(name);
            } catch (ClassNotFoundException err) {
                if (log.isDebugEnabled()) {
                    log.debug(
                            "findClass class {} not found here {}: {}", name, this, err.toString());
                }
                throw err;
            }
        }
    }

    @Data
    final class PackageMetadata {
        private final Path nar;
        private final String name;
        private final Set<String> agentTypes;
        private final Set<String> assetTypes;
        private final Set<String> streamingClusterTypes;
        private Path directory;
        private URLClassLoader classLoader;

        public PackageMetadata(
                Path nar,
                String name,
                Set<String> agentTypes,
                Set<String> assetTypes,
                Set<String> streamingClusterTypes) {
            this.nar = nar;
            this.name = name;
            this.agentTypes = agentTypes;
            this.assetTypes = assetTypes;
            this.streamingClusterTypes = streamingClusterTypes;
        }

        public synchronized void unpack() throws Exception {
            if (directory != null) {
                return;
            }
            Path dest = temporaryDirectory.resolve(nar.getFileName().toString() + ".dir");
            log.info("Unpacking NAR file {} to {}", nar, dest);
            GenericZipFileArchiveFile file = new LocalZipFileArchiveFile(nar);
            file.extractTo(dest);
            directory = dest;
        }

        interface ClassloaderBuilder {
            URLClassLoader apply(PackageMetadata metadata) throws Exception;
        }

        public synchronized URLClassLoader buildClassloader(ClassloaderBuilder builder)
                throws Exception {
            if (this.classLoader != null) {
                return classLoader;
            }
            unpack();
            this.classLoader = builder.apply(this);
            return classLoader;
        }
    }

    public synchronized void scan() throws Exception {
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
        if (packages.containsKey(filename)) {
            log.error(
                    "NarFileHandler ID: {} The package {} has already been processed",
                    System.identityHashCode(this),
                    filename);
            log.error(
                    "NarFileHandler ID: {} Current packages: {}",
                    System.identityHashCode(this),
                    packages.keySet());
            throw new IllegalStateException(
                    "The package " + filename + " has already been processed");
        }

        // first of all we look for an index file
        try (ZipFile zipFile = new ZipFile(narFile.toFile())) {

            List<String> agents = List.of();
            List<String> assetTypes = List.of();
            List<String> streamingClusterTypes = List.of();

            ZipEntry entryAgentsIndex = zipFile.getEntry("META-INF/ai.langstream.agents.index");
            if (entryAgentsIndex != null) {
                InputStream inputStream = zipFile.getInputStream(entryAgentsIndex);
                byte[] bytes = inputStream.readAllBytes();
                String string = new String(bytes, StandardCharsets.UTF_8);
                BufferedReader reader = new BufferedReader(new StringReader(string));
                agents = reader.lines().filter(s -> !s.isBlank() && !s.startsWith("#")).toList();
                log.debug(
                        "The file {} contains a static agents index, skipping the unpacking. It is expected that handles these agents: {}",
                        narFile,
                        agents);
            }

            ZipEntry entryAssetsIndex = zipFile.getEntry("META-INF/ai.langstream.assets.index");
            if (entryAssetsIndex != null) {
                InputStream inputStream = zipFile.getInputStream(entryAssetsIndex);
                byte[] bytes = inputStream.readAllBytes();
                String string = new String(bytes, StandardCharsets.UTF_8);
                BufferedReader reader = new BufferedReader(new StringReader(string));
                assetTypes =
                        reader.lines().filter(s -> !s.isBlank() && !s.startsWith("#")).toList();
                log.debug(
                        "The file {} contains a static assetTypes index, skipping the unpacking. It is expected that handles these assetTypes: {}",
                        narFile,
                        assetTypes);
            }

            ZipEntry streamingClustersIndex =
                    zipFile.getEntry("META-INF/ai.langstream.streamingClusters.index");
            if (streamingClustersIndex != null) {
                InputStream inputStream = zipFile.getInputStream(streamingClustersIndex);
                byte[] bytes = inputStream.readAllBytes();
                String string = new String(bytes, StandardCharsets.UTF_8);
                BufferedReader reader = new BufferedReader(new StringReader(string));
                streamingClusterTypes =
                        reader.lines().filter(s -> !s.isBlank() && !s.startsWith("#")).toList();
                log.debug(
                        "The file {} contains a static streamingClusters index, skipping the unpacking. It is expected that handles these streamingClusters: {}",
                        narFile,
                        assetTypes);
            }

            if (!agents.isEmpty() || !assetTypes.isEmpty() || !streamingClusterTypes.isEmpty()) {
                PackageMetadata metadata =
                        new PackageMetadata(
                                narFile,
                                filename,
                                Set.copyOf(agents),
                                Set.copyOf(assetTypes),
                                Set.copyOf(streamingClusterTypes));
                packages.put(filename, metadata);
                return;
            }

            ZipEntry serviceProviderForAgents =
                    zipFile.getEntry(
                            "META-INF/services/ai.langstream.api.runner.code.AgentCodeProvider");
            ZipEntry serviceProviderForAssets =
                    zipFile.getEntry(
                            "META-INF/services/ai.langstream.api.runner.assets.AssetManagerProvider");
            ZipEntry serviceProviderForStreamingClusters =
                    zipFile.getEntry(
                            "META-INF/services/ai.langstream.api.runner.topics.TopicConnectionProvider");
            if (serviceProviderForAgents == null
                    && serviceProviderForAssets == null
                    && serviceProviderForStreamingClusters == null) {
                log.debug(
                        "The file {} does not contain any AgentCodeProvider/AssetManagerProvider/TopicConnectionProvider, skipping the file",
                        narFile);
                return;
            }
        }

        log.debug("The file {} does not contain any indexes, still adding the file", narFile);
        PackageMetadata metadata = new PackageMetadata(narFile, filename, null, null, null);
        packages.put(filename, metadata);
    }

    @Override
    public AssetManagerRegistry.AssetPackage loadPackageForAsset(String assetType)
            throws Exception {
        PackageMetadata packageForAssetType = getPackageForAssetType(assetType);
        if (packageForAssetType == null) {
            return null;
        }
        URLClassLoader classLoader =
                createClassloaderForPackage(
                        customLibClasspath, packageForAssetType, parentClassloader);
        log.debug("For package {}, classloader {}", packageForAssetType.getName(), classLoader);
        return new AssetManagerRegistry.AssetPackage() {
            @Override
            public ClassLoader getClassloader() {
                return classLoader;
            }

            @Override
            public String getName() {
                return packageForAssetType.getName();
            }
        };
    }

    @Override
    public TopicConnectionsRuntimeRegistry.TopicConnectionsRuntimePackage
            loadPackageForTopicConnectionRuntime(String type) throws Exception {
        PackageMetadata packageForStreamingCluster = getPackageForStreamingClusterType(type);
        if (packageForStreamingCluster == null) {
            return null;
        }
        URLClassLoader classLoader =
                createClassloaderForPackage(
                        customLibClasspath, packageForStreamingCluster, parentClassloader);
        log.debug(
                "For package {}, classloader {}, parent {}",
                packageForStreamingCluster.getName(),
                classLoader,
                classLoader.getParent());
        return new TopicConnectionsRuntimeRegistry.TopicConnectionsRuntimePackage() {
            @Override
            public ClassLoader getClassloader() {
                return classLoader;
            }

            @Override
            public String getName() {
                return packageForStreamingCluster.getName();
            }
        };
    }

    @Override
    @SneakyThrows
    public AgentCodeRegistry.AgentPackage loadPackageForAgent(String agentType) {
        PackageMetadata packageForAgentType = getPackageForAgentType(agentType);
        if (packageForAgentType == null) {
            return null;
        }
        URLClassLoader classLoader =
                createClassloaderForPackage(
                        customLibClasspath, packageForAgentType, parentClassloader);
        log.debug(
                "For package {}, classloader {}, parent {}",
                packageForAgentType.getName(),
                classLoader,
                classLoader.getParent());
        return new AgentCodeRegistry.AgentPackage() {
            @Override
            public ClassLoader getClassloader() {
                return classLoader;
            }

            @Override
            public String getName() {
                return packageForAgentType.getName();
            }
        };
    }

    public PackageMetadata getPackageForAgentType(String name) {
        return packages.values().stream()
                .filter(p -> p.agentTypes != null && p.agentTypes.contains(name))
                .findFirst()
                .orElse(null);
    }

    public PackageMetadata getPackageForAssetType(String name) {
        return packages.values().stream()
                .filter(p -> p.assetTypes != null && p.assetTypes.contains(name))
                .findFirst()
                .orElse(null);
    }

    public PackageMetadata getPackageForStreamingClusterType(String name) {
        return packages.values().stream()
                .filter(
                        p ->
                                p.streamingClusterTypes != null
                                        && p.streamingClusterTypes.contains(name))
                .findFirst()
                .orElse(null);
    }

    @Override
    public List<? extends ClassLoader> getAllClassloaders() throws Exception {
        if (classloaders != null) {
            return classloaders;
        }

        classloaders = new ArrayList<>();
        for (PackageMetadata metadata : packages.values()) {
            URLClassLoader result =
                    createClassloaderForPackage(customLibClasspath, metadata, parentClassloader);
            classloaders.add(result);
        }
        return classloaders;
    }

    private ClassLoader systemClassloaderWithCustomLib;

    @Override
    public ClassLoader getSystemClassloader() {
        if (systemClassloaderWithCustomLib != null) {
            return systemClassloaderWithCustomLib;
        }
        if (customLibClasspath != null && !customLibClasspath.isEmpty()) {
            systemClassloaderWithCustomLib =
                    new NarFileClassLoader(
                            "system-classloader-with-custom-lib",
                            customLibClasspath,
                            parentClassloader);
        } else {
            systemClassloaderWithCustomLib = parentClassloader;
        }
        return systemClassloaderWithCustomLib;
    }

    private static URLClassLoader createClassloaderForPackage(
            List<URL> customLibClasspath,
            PackageMetadata packageMetadata,
            ClassLoader parentClassloader)
            throws Exception {

        return packageMetadata.buildClassloader(
                (metadata) -> {
                    log.info(
                            "Creating classloader for package {} id {}",
                            metadata.name,
                            System.identityHashCode(metadata));
                    List<URL> urls = new ArrayList<>();

                    log.debug("Adding agents code {}", metadata.directory);
                    urls.add(metadata.directory.toFile().toURI().toURL());

                    Path metaInfDirectory = metadata.directory.resolve("META-INF");
                    if (Files.isDirectory(metaInfDirectory)) {

                        Path dependencies = metaInfDirectory.resolve("bundled-dependencies");
                        if (Files.isDirectory(dependencies)) {
                            try (DirectoryStream<Path> allFiles =
                                    Files.newDirectoryStream(dependencies)) {
                                for (Path file : allFiles) {
                                    if (file.getFileName().toString().endsWith(".jar")) {
                                        urls.add(file.toUri().toURL());
                                    }
                                }
                            }
                        }
                    }

                    URLClassLoader result =
                            new NarFileClassLoader(metadata.name, urls, parentClassloader);

                    if (!customLibClasspath.isEmpty()) {
                        result =
                                new NarFileClassLoader(
                                        metadata.name + "+custom-lib", customLibClasspath, result);
                    }

                    metadata.classLoader = result;

                    return result;
                });
    }
}
