package com.datastax.oss.sga.cli.commands.applications;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import java.util.function.BiFunction;
import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(name = "list",
        description = "List all SGA applications")
public class ListApplicationCmd extends BaseApplicationCmd {

    protected static final String[] COLUMNS_FOR_RAW = {"id", "streaming", "compute", "status", "agents", "runners"};
    @CommandLine.Option(names = {"-o"}, description = "Output format")
    private Formats format = Formats.raw;

    @Override
    @SneakyThrows
    public void run() {
        final String body = http(newGet(tenantAppPath(""))).body();
        print(format, body, COLUMNS_FOR_RAW, getRawFormatValuesSupplier());
    }

    public static BiFunction<JsonNode, String, Object> getRawFormatValuesSupplier() {
        return new BiFunction<JsonNode, String, Object>() {
            @Override
            public Object apply(JsonNode jsonNode, String s) {
                switch (s) {
                    case "id":
                        return searchValueInJson(jsonNode, "applicationId");
                    case "streaming":
                        return searchValueInJson(jsonNode, "instance.instance.streamingCluster.type");
                    case "compute":
                        return searchValueInJson(jsonNode, "instance.instance.computeCluster.type");
                    case "status":
                        return searchValueInJson(jsonNode, "status.status.status");
                    case "agents": {
                        int countDeployed = 0;
                        final Map<String, Object> agents =
                                (Map<String, Object>) searchValueInJson(jsonNode, "status.agents");
                        for (Map.Entry<String, Object> stringObjectEntry : agents.entrySet()) {
                            final Object status = searchValueInJson(stringObjectEntry.getValue(), "status.status");
                            if (status != null && status.toString().equals("DEPLOYED")) {
                                countDeployed++;
                            }
                        }
                        return "%d/%d".formatted(countDeployed, agents.size());
                    }
                    case "runners": {
                        int countRunning = 0;
                        int countAll = 0;
                        final Map<String, Object> agents =
                                (Map<String, Object>) searchValueInJson(jsonNode, "status.agents");
                        for (Map.Entry<String, Object> stringObjectEntry : agents.entrySet()) {
                            final Map<String, Object> workers =
                                    (Map<String, Object>) searchValueInJson(stringObjectEntry.getValue(), "workers");
                            if (workers == null) {
                                continue;
                            }
                            for (Map.Entry<String, Object> objectEntry : workers.entrySet()) {
                                countAll++;
                                final Object status = searchValueInJson(objectEntry.getValue(), "status");
                                if (status != null && status.toString().equals("RUNNING")) {
                                    countRunning++;
                                }
                            }
                        }
                        if (countAll == 0) {
                            return "";
                        }
                        return "%d/%d".formatted(countRunning, countAll);
                    }
                    default:
                        return jsonNode.get(s);
                }
            }
        };
    }
}
