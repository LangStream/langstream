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
package ai.langstream.webservice.config;

import jakarta.validation.constraints.NotBlank;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "application.storage")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class StorageProperties {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AppsStoreProperties {
        private String type;
        private Map<String, Object> configuration = new HashMap<>();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CodeStorageProperties {
        private String type;
        private Map<String, Object> configuration = new HashMap<>();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class GlobalMetadataStoreProperties {
        private String type;
        private Map<String, Object> configuration = new HashMap<>();
    }

    @NotBlank private AppsStoreProperties apps;
    private GlobalMetadataStoreProperties global;

    private CodeStorageProperties code;
}
