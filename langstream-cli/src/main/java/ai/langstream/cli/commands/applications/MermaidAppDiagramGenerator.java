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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.SneakyThrows;

public class MermaidAppDiagramGenerator {
    static ObjectMapper mapper = new ObjectMapper();

    @SneakyThrows
    public static String generate(String applicationDescription) {
        final ApplicationModel model = new ApplicationModel();

        final Map map = mapper.readValue(applicationDescription, Map.class);
        final Map<String, Object> application = (Map<String, Object>) map.get("application");
        applyGateway(model, application);
        applyResources(model, application);

        applyPipelines(model, application);

        StringBuilder mermaid = new StringBuilder("flowchart TB\n");
        mermaid.append("\n");

        mermaid.append("subgraph resources[\"<b>Resources</b>\"]\n");
        model.getResources()
                .forEach(
                        r -> {
                            final String label =
                                    r.getType().equals("datasource")
                                            ? "[(\"" + r.getLabel() + "\")]"
                                            : "[\"Service: " + r.getLabel() + "\"]";
                            mermaid.append(r.getId().replace(" ", "")).append(label).append("\n");
                        });
        mermaid.append("end\n");
        mermaid.append("\n");

        mermaid.append("subgraph streaming-cluster[\"<b>Streaming</b>\"]\n");
        model.getTopics().forEach(t -> mermaid.append(t).append("\n"));
        model.getPipelines()
                .forEach(
                        p -> {
                            p.getAgents()
                                    .forEach(
                                            a -> {
                                                if (a.getInputTopic() != null) {
                                                    mermaid.append(a.getInputTopic())
                                                            .append(" --> ")
                                                            .append(a.getId())
                                                            .append("\n");
                                                }
                                            });
                        });
        mermaid.append("end\n");
        mermaid.append("\n");

        mermaid.append("subgraph gateways[\"<b>Gateways</b>\"]\n");
        model.getGateways()
                .forEach(
                        g -> {
                            for (String topic : g.getTopics()) {
                                mermaid.append(g.getId())
                                        .append(" --> ")
                                        .append(topic)
                                        .append("\n");
                            }
                        });
        mermaid.append("end\n");
        mermaid.append("\n");

        model.getPipelines()
                .forEach(
                        p -> {
                            mermaid.append("subgraph ")
                                    .append(p.getId())
                                    .append("[\"<b>")
                                    .append(p.getId())
                                    .append("</b>\"]\n");
                            p.getAgents()
                                    .forEach(
                                            a -> {
                                                if (a.getOutput() != null) {
                                                    for (String output : a.getOutput()) {
                                                        mermaid.append(a.getId())
                                                                .append("(\"")
                                                                .append(a.getLabel())
                                                                .append("\")")
                                                                .append(" --> ")
                                                                .append(output)
                                                                .append("\n");
                                                    }
                                                }
                                            });
                            mermaid.append("end\n\n");
                        });
        return mermaid.toString();
    }

    private static void applyResources(ApplicationModel model, Map<String, Object> application) {
        ((Map<String, Object>) asMap(application.get("resources")))
                .entrySet()
                .forEach(
                        entry -> {
                            final Map resource = asMap(entry.getValue());
                            final String type;
                            if (resource.get("type").equals("datasource")) {
                                type = "datasource";
                            } else {
                                type = "service";
                            }
                            model.getResources()
                                    .add(
                                            new Resource(
                                                    entry.getKey(),
                                                    (String) resource.get("name"),
                                                    type,
                                                    (String) resource.get("type")));
                        });
    }

    private static void applyPipelines(ApplicationModel model, Map<String, Object> application) {
        asList(application.get("modules"))
                .forEach(
                        module -> {
                            applyTopics(model, asMap(module));

                            asList(asMap(module).get("pipelines"))
                                    .forEach(
                                            p -> {
                                                final Map pipeline = asMap(p);
                                                List<Agent> agents = new ArrayList<>();

                                                for (Object ag : asList(pipeline.get("agents"))) {

                                                    final Map agentMap = asMap(ag);
                                                    final Agent agent = new Agent();
                                                    agent.setId((String) agentMap.get("id"));
                                                    final String agentType =
                                                            (String) agentMap.get("type");
                                                    agent.setType(agentType);
                                                    agent.setLabel((String) agentMap.get("name"));
                                                    final Map input = asMap(agentMap.get("input"));
                                                    if (input != null) {
                                                        if (input.get("connectionType")
                                                                .equals("TOPIC")) {
                                                            agent.setInputTopic(
                                                                    (String)
                                                                            input.get(
                                                                                    "definition"));
                                                        }
                                                    } else {
                                                        agent.setExternalInput(
                                                                model.getSourceExternal());
                                                    }

                                                    final Map output =
                                                            asMap(agentMap.get("output"));

                                                    List<String> outputs = new ArrayList<>();
                                                    if (output != null) {
                                                        outputs.add(
                                                                (String) output.get("definition"));
                                                    }
                                                    final Map agentConfig =
                                                            asMap(agentMap.get("configuration"));
                                                    final Object datasource =
                                                            agentConfig.get("datasource");
                                                    if (datasource != null
                                                            && datasource instanceof String) {
                                                        outputs.add((String) datasource);
                                                    }

                                                    final Object streamToTopic =
                                                            agentConfig.get("stream-to-topic");
                                                    if (streamToTopic != null
                                                            && streamToTopic instanceof String) {
                                                        outputs.add((String) streamToTopic);
                                                    }
                                                    switch (agentType) {
                                                        case "ai-text-completions":
                                                        case "ai-chat-completions":
                                                        case "compute-ai-embeddings":
                                                            if (agentConfig.containsKey(
                                                                    "ai-service")) {
                                                                outputs.add(
                                                                        (String)
                                                                                agentConfig.get(
                                                                                        "ai-service"));
                                                            } else {
                                                                final Resource resource =
                                                                        model
                                                                                .getResources()
                                                                                .stream()
                                                                                .filter(
                                                                                        r ->
                                                                                                r.getOriginalType()
                                                                                                                .equals(
                                                                                                                        "vertex-configuration")
                                                                                                        || r.getOriginalType()
                                                                                                                .equals(
                                                                                                                        "open-ai-configuration")
                                                                                                        || r.getOriginalType()
                                                                                                                .equals(
                                                                                                                        "hugging-face-configuration"))
                                                                                .findFirst()
                                                                                .orElse(null);
                                                                if (resource != null) {
                                                                    outputs.add(
                                                                            resource.getId()
                                                                                    .replace(
                                                                                            " ",
                                                                                            ""));
                                                                }
                                                            }
                                                            break;
                                                        default:
                                                            break;
                                                    }

                                                    agent.setOutput(outputs);
                                                    agents.add(agent);
                                                }
                                                model.getPipelines()
                                                        .add(
                                                                new Pipeline(
                                                                        (String) pipeline.get("id"),
                                                                        agents));
                                            });
                        });
    }

    private static void applyTopics(ApplicationModel model, Map<String, Object> module) {
        asList(module.get("topics"))
                .forEach(t -> model.getTopics().add((String) asMap(t).get("name")));
    }

    private static void applyGateway(ApplicationModel model, Map<String, Object> application) {
        final List<Object> gateways = asList(asMap(application.get("gateways")).get("gateways"));
        if (gateways != null) {
            gateways.forEach(
                    e -> {
                        final Map gw = asMap(e);
                        // TODO: handle chat
                        model.getGateways()
                                .add(
                                        new Gateway(
                                                (String) gw.get("id"),
                                                List.of((String) gw.get("topic"))));
                    });
        }
    }

    private static Map asMap(Object e) {
        return mapper.convertValue(e, Map.class);
    }

    private static List asList(Object e) {
        return mapper.convertValue(e, List.class);
    }

    @Data
    @AllArgsConstructor
    private static class Gateway {
        String id;
        List<String> topics;
    }

    @Data
    @AllArgsConstructor
    private static class Resource {
        String id;
        String label;
        String type;
        String originalType;
    }

    @Data
    private static class Agent {
        String id;
        String type;
        String label;
        String inputTopic;
        String externalInput;
        List<String> output;
    }

    @Data
    @AllArgsConstructor
    private static class Pipeline {
        String id;
        List<Agent> agents;
    }

    @Data
    private static class ApplicationModel {
        List<String> topics = new ArrayList<>();
        List<Gateway> gateways = new ArrayList<>();
        List<Pipeline> pipelines = new ArrayList<>();
        List<Resource> resources = new ArrayList<>();
        List<String> globals = new ArrayList<>();

        String getSourceExternal() {
            if (!globals.contains("external-source")) {
                globals.add("external-source");
            }
            return "external-source";
        }
    }
}
