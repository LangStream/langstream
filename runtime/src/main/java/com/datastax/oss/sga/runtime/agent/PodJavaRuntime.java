package com.datastax.oss.sga.runtime.agent;

import com.datastax.oss.sga.api.runner.code.AgentCode;
import com.datastax.oss.sga.api.runner.code.AgentCodeRegistry;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.topics.TopicConnectionsRuntime;
import com.datastax.oss.sga.api.runner.topics.TopicConnectionsRuntimeRegistry;
import com.datastax.oss.sga.api.runner.topics.TopicConsumer;
import com.datastax.oss.sga.api.runner.topics.TopicProducer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.util.List;

/**
 * This is the main entry point for the pods that run the SGA runtime and Java code.
 */
@Slf4j
public class PodJavaRuntime
{
    private static final TopicConnectionsRuntimeRegistry TOPIC_CONNECTIONS_REGISTRY = new TopicConnectionsRuntimeRegistry();
    private static final AgentCodeRegistry AGENT_CODE_REGISTRY = new AgentCodeRegistry();
    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

    private static ErrorHandler errorHandler = error -> {
        log.error("Unexpected error", error);
        System.exit(-1);
    };

    public interface  ErrorHandler {
        void handleError(Throwable error);
    }

    public static void main(String ... args) {
        try {
            if (args.length < 1) {
                throw new IllegalArgumentException("Missing pod configuration file argument");
            }
            Path podRuntimeConfiguration = Path.of(args[0]);
            log.info("Loading pod configuration from {}", podRuntimeConfiguration);

            // TODO: resolve placeholders and secrets
            RuntimePodConfiguration configuration = MAPPER.readValue(podRuntimeConfiguration.toFile(),
                    RuntimePodConfiguration.class);

            run(configuration, -1);


        } catch (Throwable error) {
            errorHandler.handleError(error);
        }
    }

    public static void run(RuntimePodConfiguration configuration, int maxLoops) throws Exception {
        log.info("Pod Configuration {}", configuration);

        // agentId is the identity of the agent in the cluster
        // it is shared by all the instances of the agent
        String agentId = configuration.agent().applicationId() + "-" + configuration.agent().agentId();

        log.info("Starting agent {} with configuration {}", agentId, configuration.agent());

        TopicConnectionsRuntime topicConnectionsRuntime =
                TOPIC_CONNECTIONS_REGISTRY.getTopicConnectionsRuntime(configuration.streamingCluster());

        log.info("TopicConnectionsRuntime {}", topicConnectionsRuntime);

        AgentCode agentCode = initAgent(configuration);

        TopicConsumer consumer = new TopicConsumer() {
            @SneakyThrows

            @Override
            public List<Record> read() {
                log.info("Sleeping for 1 second, no records...");
                Thread.sleep(1000);
                return List.of();
            }
        };
        if (configuration.input() != null && !configuration.input().isEmpty()) {
            consumer = topicConnectionsRuntime.createConsumer(agentId,
                    configuration.streamingCluster(), configuration.input());
        }

        TopicProducer producer = new TopicProducer() {};
        if (configuration.output() != null && !configuration.output().isEmpty()) {
            producer = topicConnectionsRuntime.createProducer(agentId, configuration.streamingCluster(), configuration.output());
        }



        runMainLoop(consumer, producer, agentCode, maxLoops);
    }

    private static void runMainLoop(TopicConsumer consumer, TopicProducer producer, AgentCode agentCode, int maxLoops) throws Exception {
        try {
            consumer.start();
            producer.start();
            agentCode.start();
            // TODO: handle semantics, transactions...
            List<Record> records = consumer.read();
            while ((maxLoops < 0) || (maxLoops-- > 0)) {
                if (records != null && !records.isEmpty()) {
                    try {
                        List<Record> outputRecords = agentCode.process(records);
                        producer.write(outputRecords);
                    } catch (Exception e) {
                        log.error("Error while processing records", e);

                        // throw the error
                        // this way the consumer will not commit the records
                        throw new RuntimeException("Error while processing records", e);
                    }
                    // commit
                }
                consumer.commit();
                records = consumer.read();
            }
        } finally {
            consumer.close();
            producer.close();
            agentCode.close();
        }
    }

    private static AgentCode initAgent(RuntimePodConfiguration configuration) throws Exception {
        log.info("Bootstrapping agent with configuration {}", configuration.agent());
        AgentCode agentCode = AGENT_CODE_REGISTRY.getAgentCode(configuration.agent().agentType());
        agentCode.init(configuration.agent().configuration());
        return agentCode;
    }

    public static ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    public static void setErrorHandler(ErrorHandler errorHandler) {
        PodJavaRuntime.errorHandler = errorHandler;
    }
}
