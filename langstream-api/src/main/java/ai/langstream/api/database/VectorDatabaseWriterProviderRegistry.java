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
package ai.langstream.api.database;

import java.util.Map;
import java.util.ServiceLoader;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class VectorDatabaseWriterProviderRegistry {

    public static VectorDatabaseWriter createWriter(Map<String, Object> configuration) {
        Map<String, Object> conf = configuration == null ? Map.of() : configuration;
        ServiceLoader<VectorDatabaseWriterProvider> loader =
                ServiceLoader.load(VectorDatabaseWriterProvider.class);
        final VectorDatabaseWriterProvider store =
                loader.stream()
                        .filter(p -> p.get().supports(conf))
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "No VectorDatabaseWriterProvider found for datasource "
                                                        + configuration))
                        .get();
        return store.createImplementation(configuration);
    }
}
