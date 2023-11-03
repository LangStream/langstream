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
package ai.langstream.cli.commands.applications;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(name = "list", header = "List all applications")
public class ListApplicationCmd extends BaseApplicationCmd {

    protected static final String[] COLUMNS_FOR_RAW = {
        "id", "streaming", "compute", "status", "executors", "replicas"
    };

    @CommandLine.Option(
            names = {"-o"},
            description = "Output format. Formats are: yaml, json, raw. Default value is raw.")
    private Formats format = Formats.raw;

    @Override
    @SneakyThrows
    public void run() {
        ensureFormatIn(format, Formats.raw, Formats.json, Formats.yaml);
        final String body = getClient().applications().list();
        print(format, body, COLUMNS_FOR_RAW, getRawFormatValuesSupplier());
    }

    public static BiFunction<JsonNode, String, Object> getRawFormatValuesSupplier() {
        return (jsonNode, s) -> {
            switch (s) {
                case "id":
                    return searchValueInJson(jsonNode, "application-id");
                case "streaming":
                    return searchValueInJson(
                            jsonNode, "application.instance.streamingCluster.type");
                case "compute":
                    return searchValueInJson(jsonNode, "application.instance.computeCluster.type");
                case "status":
                    return searchValueInJson(jsonNode, "status.status.status");
                case "executors":
                    {
                        int countDeployed = 0;
                        final List<Map<String, Object>> executors =
                                (List<Map<String, Object>>)
                                        searchValueInJson(jsonNode, "status.executors");
                        for (Map<String, Object> stringObjectEntry : executors) {
                            final Object status =
                                    searchValueInJson(stringObjectEntry, "status.status");
                            if (status != null && status.toString().equals("DEPLOYED")) {
                                countDeployed++;
                            }
                        }
                        return String.format("%d/%d", countDeployed, executors.size());
                    }
                case "replicas":
                    {
                        int countRunning = 0;
                        int countAll = 0;
                        final List<Map<String, Object>> executors =
                                (List<Map<String, Object>>)
                                        searchValueInJson(jsonNode, "status.executors");
                        for (Map<String, Object> stringObjectEntry : executors) {
                            final List<Map<String, Object>> replicas =
                                    (List<Map<String, Object>>) stringObjectEntry.get("replicas");
                            if (replicas == null) {
                                continue;
                            }
                            for (Map<String, Object> objectEntry : replicas) {
                                countAll++;
                                final Object status = objectEntry.get("status");
                                if (status != null && status.toString().equals("RUNNING")) {
                                    countRunning++;
                                }
                            }
                        }
                        if (countAll == 0) {
                            return "";
                        }
                        return String.format("%d/%d", countRunning, countAll);
                    }
                default:
                    return jsonNode.get(s);
            }
        };
    }
}
