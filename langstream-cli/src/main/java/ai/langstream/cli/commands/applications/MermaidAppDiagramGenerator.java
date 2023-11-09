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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
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

        StringBuilder mermaid = new StringBuilder("flowchart LR\n");
        mermaid.append("\n");

        // first declare all resources without connections
        if (!model.getGateways().isEmpty()) {
            mermaid.append("external-client((Client))\n\n");
        }

        for (External externalSink : model.getExternalSinks()) {
            mermaid.append(externalSink.getId())
                    .append("([\"")
                    .append(externalSink.getLabel())
                    .append("\"])")
                    .append("\n\n");
        }
        for (External source : model.getExternalSources()) {
            mermaid.append(source.getId())
                    .append("([\"")
                    .append(source.getLabel())
                    .append("\"])")
                    .append("\n\n");
        }

        mermaid.append("subgraph resources[\"Resources\"]\n");
        model.getResources()
                .forEach(
                        r -> {
                            final String label =
                                    r.getType().equals("datasource")
                                            ? "[(\"" + r.getLabel() + "\")]"
                                            : "(\"" + r.getLabel() + "\")";
                            mermaid.append(escapeId(IDType.RESOURCE, r.getId()))
                                    .append(label)
                                    .append("\n");
                        });
        mermaid.append("end\n");
        mermaid.append("\n");

        mermaid.append("subgraph streaming-cluster[\"Topics\"]\n");
        model.getTopics()
                .forEach(
                        t ->
                                mermaid.append(t.getId())
                                        .append("([\"")
                                        .append(t.getLabel())
                                        .append("\"])")
                                        .append("\n"));
        mermaid.append("end\n");
        mermaid.append("\n");

        mermaid.append("subgraph gateways[\"Gateways\"]\n");
        model.getGateways()
                .forEach(
                        t ->
                                mermaid.append(t.getId())
                                        .append("[/\"")
                                        .append(t.getLabel())
                                        .append("\"\\]")
                                        .append("\n"));
        mermaid.append("end\n");
        mermaid.append("\n");

        model.getPipelines()
                .forEach(
                        p -> {
                            mermaid.append("subgraph pipeline-")
                                    .append(p.getId())
                                    .append("[\"Pipeline: <b>")
                                    .append(p.getId())
                                    .append("</b>\"]\n");
                            p.getAgents()
                                    .forEach(
                                            a -> {
                                                mermaid.append(a.getId())
                                                        .append("(\"")
                                                        .append(a.getLabel())
                                                        .append("\")")
                                                        .append("\n");
                                            });
                            mermaid.append("end\n\n");
                        });

        // now declare all the connections
        AtomicInteger countConnection = new AtomicInteger();
        model.getPipelines()
                .forEach(
                        p -> {
                            p.getAgents()
                                    .forEach(
                                            a -> {
                                                if (a.getInput() != null) {
                                                    addConnection(
                                                            mermaid,
                                                            countConnection,
                                                            a.getId(),
                                                            a.getInput(),
                                                            true,
                                                            "stroke:#82E0AA");
                                                }
                                            });
                        });

        model.getGateways()
                .forEach(
                        g -> {
                            for (String topic : g.getConsumeFromTopics()) {
                                addConnection(
                                        mermaid, countConnection, g.getId(), topic, true, null);
                            }

                            for (String topic : g.getProduceToTopics()) {
                                addConnection(
                                        mermaid, countConnection, g.getId(), topic, false, null);
                            }
                        });

        addConnection(mermaid, countConnection, "external-client", "gateways", false, null);

        model.getPipelines()
                .forEach(
                        p -> {
                            p.getAgents()
                                    .forEach(
                                            a -> {
                                                if (a.getOutput() != null) {
                                                    List<String> endOutputs = new ArrayList<>();
                                                    for (String output : a.getOutput()) {
                                                        if (output.startsWith("topic-")
                                                                || output.startsWith(
                                                                        "external-sink-")) {
                                                            endOutputs.add(output);
                                                        }
                                                    }

                                                    for (String output : a.getOutput()) {
                                                        if (endOutputs.contains(output)
                                                                || (endOutputs.isEmpty()
                                                                        && output.startsWith(
                                                                                "resource-"))) {
                                                            addConnection(
                                                                    mermaid,
                                                                    countConnection,
                                                                    a.getId(),
                                                                    output,
                                                                    false,
                                                                    "stroke:#F4D03F");
                                                        } else {
                                                            if (output.startsWith("resource-")) {
                                                                addConnection(
                                                                        mermaid,
                                                                        countConnection,
                                                                        a.getId(),
                                                                        output,
                                                                        true,
                                                                        "stroke:#5DADE2");
                                                            } else {
                                                                addConnection(
                                                                        mermaid,
                                                                        countConnection,
                                                                        a.getId(),
                                                                        output,
                                                                        false,
                                                                        "stroke:#5DADE2");
                                                            }
                                                        }
                                                    }
                                                }
                                            });
                        });
        return mermaid.toString();
    }

    private static void addConnection(
            StringBuilder mermaid,
            AtomicInteger counter,
            String from,
            String to,
            boolean dotted,
            String linkStyle) {
        final int count = counter.getAndIncrement();
        final String link = dotted ? "-.->" : "-->";
        mermaid.append(from).append(link).append(to).append("\n");
        if (linkStyle != null) {
            mermaid.append("linkStyle ").append(count).append(" ").append(linkStyle).append("\n");
        }
    }

    enum IDType {
        TOPIC,
        GATEWAY,
        AGENT,
        RESOURCE
    }

    private static String escapeId(IDType idType, String string) {
        return idType.name().toLowerCase() + "-" + string.replace(" ", "___").toLowerCase();
    }

    private static void applyResources(ApplicationModel model, Map<String, Object> application) {
        final Map<String, Object> resources = asMap(application.get("resources"));
        if (resources == null) {
            return;
        }
        resources
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
        final List modules = asList(application.get("modules"));
        if (modules == null) {
            return;
        }
        modules.forEach(
                module -> {
                    applyTopics(model, asMap(module));

                    asList(asMap(module).get("pipelines"))
                            .forEach(
                                    p -> {
                                        final Map pipeline = asMap(p);
                                        List<Agent> agents = new ArrayList<>();

                                        for (Object ag : asList(pipeline.get("agents"))) {

                                            final Map agentMap = asMap(ag);
                                            final Agent agent = parseAgent(model, agentMap);
                                            agents.add(agent);
                                        }
                                        final String id = (String) pipeline.get("id");
                                        model.getPipelines().add(new Pipeline(id, id, agents));
                                    });
                });
    }

    private static Agent parseAgent(ApplicationModel model, Map agentMap) {
        final Agent agent = new Agent();
        agent.setId(escapeId(IDType.AGENT, (String) agentMap.get("id")));
        final String agentType = (String) agentMap.get("type");
        agent.setType(agentType);
        agent.setLabel((String) agentMap.get("name"));
        final Map input = asMap(agentMap.get("input"));
        if (input != null) {
            if (input.get("connectionType").equals("TOPIC")) {
                agent.setInput(escapeId(IDType.TOPIC, (String) input.get("definition")));
            }
        } else {
            agent.setInput(model.getExternalSourceForAgent(agent.getId()).getId());
        }

        final Map output = asMap(agentMap.get("output"));

        List<String> outputs = new ArrayList<>();
        if (output != null) {
            final String definition = (String) output.get("definition");
            if (output.get("connectionType").equals("TOPIC")) {
                outputs.add(escapeId(IDType.TOPIC, definition));
            } else {
                outputs.add(escapeId(IDType.AGENT, definition));
            }
        } else {
            outputs.add(model.getExternalSinkForAgent(agent.getId()).getId());
        }
        final Map agentConfig = asMap(agentMap.get("configuration"));
        final Object datasource = agentConfig.get("datasource");
        if (datasource != null && datasource instanceof String) {
            outputs.add(escapeId(IDType.RESOURCE, (String) datasource));
        }

        final Object streamToTopic = agentConfig.get("stream-to-topic");
        if (streamToTopic != null && streamToTopic instanceof String) {
            outputs.add(escapeId(IDType.TOPIC, (String) streamToTopic));
        }
        switch (agentType) {
            case "ai-text-completions":
            case "ai-chat-completions":
            case "compute-ai-embeddings":
                if (agentConfig.containsKey("ai-service")) {
                    outputs.add(escapeId(IDType.RESOURCE, (String) agentConfig.get("ai-service")));
                } else {
                    final Resource resource =
                            model.getResources().stream()
                                    .filter(
                                            r ->
                                                    r.getOriginalType()
                                                                    .equals("vertex-configuration")
                                                            || r.getOriginalType()
                                                                    .equals("open-ai-configuration")
                                                            || r.getOriginalType()
                                                                    .equals(
                                                                            "hugging-face-configuration")
                                                            || r.getOriginalType()
                                                                    .equals("bedrock-configuration")
                                                            || r.getOriginalType()
                                                                    .equals("ollama-configuration"))
                                    .findFirst()
                                    .orElse(null);
                    if (resource != null) {
                        outputs.add(escapeId(IDType.RESOURCE, resource.getId()));
                    }
                }
                break;
            default:
                break;
        }

        agent.setOutput(outputs);
        return agent;
    }

    private static void applyTopics(ApplicationModel model, Map<String, Object> module) {
        asList(module.get("topics"))
                .forEach(
                        t -> {
                            final String topicName = (String) asMap(t).get("name");
                            model.getTopics()
                                    .add(new Topic(escapeId(IDType.TOPIC, topicName), topicName));
                        });
    }

    private static void applyGateway(ApplicationModel model, Map<String, Object> application) {
        final Map gatewaysObject = asMap(application.get("gateways"));
        if (gatewaysObject == null) {
            return;
        }
        final List<Object> gateways = asList(gatewaysObject.get("gateways"));
        if (gateways != null) {
            gateways.forEach(
                    e -> {
                        final Map gw = asMap(e);
                        List<String> produceToTopics = new ArrayList<>();
                        List<String> consumeFromTopics = new ArrayList<>();
                        final Map chatOptions = asMap(gw.get("chat-options"));

                        if (chatOptions != null) {

                            final String questionsTopic =
                                    (String) chatOptions.get("questions-topic");
                            if (questionsTopic != null) {
                                produceToTopics.add(escapeId(IDType.TOPIC, questionsTopic));
                            }
                            final String answersTopic = (String) chatOptions.get("answers-topic");
                            if (answersTopic != null) {
                                consumeFromTopics.add(escapeId(IDType.TOPIC, answersTopic));
                            }
                        } else {
                            final String topic = (String) gw.get("topic");
                            if (topic != null) {
                                if (gw.get("type").equals("produce")) {
                                    produceToTopics.add(escapeId(IDType.TOPIC, topic));
                                } else {
                                    consumeFromTopics.add(escapeId(IDType.TOPIC, topic));
                                }
                            }
                        }
                        final String id = (String) gw.get("id");
                        model.getGateways()
                                .add(
                                        new Gateway(
                                                escapeId(IDType.GATEWAY, id),
                                                id,
                                                produceToTopics,
                                                consumeFromTopics));
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
        String label;
        List<String> produceToTopics;
        List<String> consumeFromTopics;
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
        String input;
        List<String> output;
    }

    @Data
    @AllArgsConstructor
    private static class Pipeline {
        String id;
        String label;
        List<Agent> agents;
    }

    @Data
    @AllArgsConstructor
    private static class Topic {
        String id;
        String label;
    }

    @Data
    @AllArgsConstructor
    private static class External {
        String id;
        String label;
    }

    @Data
    private static class ApplicationModel {
        List<Topic> topics = new ArrayList<>();
        List<Gateway> gateways = new ArrayList<>();
        List<MermaidAppDiagramGenerator.Pipeline> pipelines = new ArrayList<>();
        List<Resource> resources = new ArrayList<>();
        Set<External> externalSources = new HashSet<>();
        Set<External> externalSinks = new HashSet<>();

        External getExternalSourceForAgent(String agentId) {
            final External source = new External("external-source-" + agentId, "External system");
            externalSources.add(source);
            return source;
        }

        External getExternalSinkForAgent(String agentId) {
            final External sink = new External("external-sink-" + agentId, "External system");
            externalSinks.add(sink);
            return sink;
        }
    }
}
