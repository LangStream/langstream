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
package ai.langstream.model.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.langstream.api.model.Application;
import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.model.Module;
import ai.langstream.impl.parser.ModelBuilder;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class AssetsTest {

    @Test
    public void testConfigureAssets() throws Exception {
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "module.yaml",
                                        """
                                module: "module-1"
                                id: "pipeline-1"
                                assets:
                                  - name: "Cassandra table"
                                    asset-type: "cassandra-table"
                                    creation-mode: create-if-not-exists
                                    config:
                                      table-name: "products"
                                      key-space: "my_keyspace"
                                      statements:
                                         - "CREATE TABLE my_keyspace.products (id text PRIMARY KEY, name text, price int)"
                                         - "CREATE INDEX ON my_keyspace.products (name, price)"
                                pipeline:
                                  - name: "step1"
                                    type: "noop"
                                """),
                                buildInstanceYaml(),
                                null)
                        .getApplication();
        Module module = applicationInstance.getModule("module-1");
        assertEquals(1, module.getAssets().size());
        AssetDefinition asset = module.getAssets().get(0);
        assertEquals("Cassandra table", asset.getName());
        assertEquals("create-if-not-exists", asset.getCreationMode());
        assertEquals("cassandra-table", asset.getAssetType());
        assertEquals(
                Map.of(
                        "table-name",
                        "products",
                        "key-space",
                        "my_keyspace",
                        "statements",
                        List.of(
                                "CREATE TABLE my_keyspace.products (id text PRIMARY KEY, name text, price int)",
                                "CREATE INDEX ON my_keyspace.products (name, price)")),
                asset.getConfig());
    }

    private static String buildInstanceYaml() {
        return """
                instance:
                  streamingCluster:
                    type: "noop"
                  computeCluster:
                    type: "none"
                """;
    }
}
