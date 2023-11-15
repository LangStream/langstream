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
package ai.langstream.deployer.k8s.api.crds.apps;

import ai.langstream.deployer.k8s.api.crds.NamespacedSpec;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

@Data
@NoArgsConstructor
public class ApplicationSpec extends NamespacedSpec {

    private static final ObjectMapper mapper =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @SneakyThrows
    public static String serializeApplication(
            SerializedApplicationInstance serializedApplicationInstance) {
        return mapper.writeValueAsString(serializedApplicationInstance);
    }

    @SneakyThrows
    public static SerializedApplicationInstance deserializeApplication(
            String serializedApplicationInstance) {
        return mapper.readValue(serializedApplicationInstance, SerializedApplicationInstance.class);
    }

    @SneakyThrows
    public static ApplicationSpecOptions deserializeOptions(String options) {
        if (options == null) {
            return new ApplicationSpecOptions();
        }
        return mapper.readValue(options, ApplicationSpecOptions.class);
    }

    @SneakyThrows
    public static String serializeOptions(ApplicationSpecOptions options) {
        return mapper.writeValueAsString(options);
    }

    @Deprecated private String image;
    @Deprecated private String imagePullPolicy;

    /**
     * {@link SerializedApplicationInstance} serialized as json. Field as string to simplify future
     * changes to the SerializedApplicationInstance schema.
     */
    private String application;

    private String codeArchiveId;
    private String options;
}
