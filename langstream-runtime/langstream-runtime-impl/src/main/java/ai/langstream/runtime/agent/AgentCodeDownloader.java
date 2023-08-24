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
package ai.langstream.runtime.agent;


import ai.langstream.api.codestorage.CodeStorage;
import ai.langstream.api.codestorage.CodeStorageRegistry;
import ai.langstream.runtime.api.agent.CodeStorageConfig;
import ai.langstream.runtime.deployer.RuntimeDeployer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.file.Path;

import lombok.extern.slf4j.Slf4j;

/**
 * This is the main entry point for downloading the application code.
 */
@Slf4j
public class AgentCodeDownloader {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static RuntimeDeployer.ErrorHandler errorHandler = error -> {
        log.error("Unexpected error", error);
        System.exit(-1);
    };

    public static void main(String... args) {
        try {
            if (args.length < 1) {
                throw new IllegalArgumentException("Missing code configuration");
            }

            Path codeConfigPath = Path.of(args[0]);
            log.info("Loading code config config from {}", codeConfigPath);
            final CodeStorageConfig codeStorageConfig =
                    MAPPER.readValue(codeConfigPath.toFile(), CodeStorageConfig.class);
            Path codeDownloadPath = Path.of(args[1]);
            downloadCustomCode(codeStorageConfig, codeDownloadPath);
            log.info("Code downloaded to {}", codeDownloadPath);
        } catch (Throwable error) {
            errorHandler.handleError(error);
            return;
        }
    }

    private static void downloadCustomCode(CodeStorageConfig codeStorageConfig, Path codeDownloadPath) throws Exception {



        if (codeStorageConfig != null) {
            log.info("Downloading custom code from {}", codeStorageConfig);
            log.info("Custom code is stored in {}", codeDownloadPath);
            try (CodeStorage codeStorage =
                    CodeStorageRegistry.getCodeStorage(codeStorageConfig.type(), codeStorageConfig.configuration());) {
                codeStorage.downloadApplicationCode(codeStorageConfig.tenant(),
                        codeStorageConfig.codeStorageArchiveId(), (downloadedCodeArchive -> {
                            downloadedCodeArchive.extractTo(codeDownloadPath);
                        }));
            }
        }
    }
}
