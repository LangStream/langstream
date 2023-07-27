package com.datastax.oss.sga.runtime.agent;

import com.datastax.oss.sga.api.runner.code.AgentCode;
import com.datastax.oss.sga.api.runner.code.AgentCodeRegistry;
import com.datastax.oss.sga.api.runner.code.AgentContext;
import com.datastax.oss.sga.api.runner.code.AgentProcessor;
import com.datastax.oss.sga.api.runner.code.AgentSink;
import com.datastax.oss.sga.api.runner.code.AgentSource;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.topics.TopicConnectionsRuntime;
import com.datastax.oss.sga.api.runner.topics.TopicConnectionsRuntimeRegistry;
import com.datastax.oss.sga.api.runner.topics.TopicConsumer;
import com.datastax.oss.sga.api.runner.topics.TopicProducer;
import com.datastax.oss.sga.runtime.agent.python.PythonCodeAgentProvider;
import com.datastax.oss.sga.runtime.agent.simple.IdentityAgentProvider;
import com.datastax.oss.sga.runtime.api.agent.RuntimePodConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * This is the main entry point for the pods that run the SGA runtime and Java code.
 */
@Slf4j
public class AgentRunner
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

    @SneakyThrows
    public static void main(String ... args) {
        try {
            if (args.length < 1) {
                throw new IllegalArgumentException("Missing pod configuration file argument");
            }
            Path podRuntimeConfiguration = Path.of(args[0]);
            log.info("Loading pod configuration from {}", podRuntimeConfiguration);

            RuntimePodConfiguration configuration = MAPPER.readValue(podRuntimeConfiguration.toFile(),
                    RuntimePodConfiguration.class);


            Path codeDirectory = Path.of(args[1]);
            log.info("Loading code from {}", codeDirectory);
            run(configuration, podRuntimeConfiguration, codeDirectory, -1);


        } catch (Throwable error) {
            log.info("Error, NOW SLEEPING", error);
            Thread.sleep(60000);
            errorHandler.handleError(error);
        }
    }


    public static void run(RuntimePodConfiguration configuration,
                           Path podRuntimeConfiguration,
                           Path codeDirectory,
                           int maxLoops) throws Exception {
        log.info("Pod Configuration {}", configuration);

        // agentId is the identity of the agent in the cluster
        // it is shared by all the instances of the agent
        String agentId = configuration.agent().applicationId() + "-" + configuration.agent().agentId();

        log.info("Starting agent {} with configuration {}", agentId, configuration.agent());

        TopicConnectionsRuntime topicConnectionsRuntime =
                TOPIC_CONNECTIONS_REGISTRY.getTopicConnectionsRuntime(configuration.streamingCluster());

        log.info("TopicConnectionsRuntime {}", topicConnectionsRuntime);
        try {
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            try {
                ClassLoader customLibClassloader = buildCustomLibClassloader(codeDirectory, contextClassLoader);
                Thread.currentThread().setContextClassLoader(customLibClassloader);
                AgentCode agentCode = initAgent(configuration);
                if (PythonCodeAgentProvider.isPythonCodeAgent(agentCode)) {
                    runPythonAgent(
                        podRuntimeConfiguration, codeDirectory);
                } else {
                    runJavaAgent(configuration, maxLoops, agentId, topicConnectionsRuntime, agentCode);
                }
            } finally {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }

        } finally {
            topicConnectionsRuntime.close();
        }
    }

    private static ClassLoader buildCustomLibClassloader(Path codeDirectory, ClassLoader contextClassLoader) throws IOException {
        ClassLoader customLibClassloader = contextClassLoader;
        if (codeDirectory == null) {
            return customLibClassloader;
        }
        Path javaLib = codeDirectory.resolve("java").resolve("lib");
        log.info("Looking for java lib in {}", javaLib);
        if (Files.exists(javaLib)&& Files.isDirectory(javaLib)) {
            List<URL> jars;
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(javaLib, "*.jar")) {
                jars = StreamSupport.stream(stream.spliterator(), false)
                        .map(p -> {
                            log.info("Adding jar {}", p);
                            try {
                                return p.toUri().toURL();
                            } catch (MalformedURLException e) {
                                log.error("Error adding jar {}", p, e);
                                // ignore
                                return null;
                            }
                        })
                        .filter(p -> p != null)
                        .collect(Collectors.toList());
                ;
            }
            customLibClassloader = new URLClassLoader(jars.toArray(URL[]::new), contextClassLoader);
        }
        return customLibClassloader;
    }

    private static void runPythonAgent(Path podRuntimeConfiguration, Path codeDirectory) throws Exception {

        Path pythonCodeDirectory = codeDirectory.resolve("python");
        log.info("Python code directory {}", pythonCodeDirectory);

        final String pythonPath = System.getenv("PYTHONPATH");
        final String newPythonPath = "%s:%s".formatted(pythonPath, pythonCodeDirectory.toAbsolutePath().toString());

        // copy input/output to standard input/output of the java process
        // this allows to use "kubectl logs" easily
        ProcessBuilder processBuilder = new ProcessBuilder(
                "python3", "-m", "sga_runtime", podRuntimeConfiguration.toAbsolutePath().toString())
                .inheritIO()
                .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                .redirectError(ProcessBuilder.Redirect.INHERIT);
        processBuilder
                .environment()
                .put("PYTHONPATH", newPythonPath);
        processBuilder
                .environment()
                .put("NLTK_DATA", "/app/nltk_data");
        Process process = processBuilder.start();

        int exitCode = process.waitFor();
        log.info("Python process exited with code {}", exitCode);

        if (exitCode != 0) {
            throw new Exception("Python code exited with code " + exitCode);
        }
    }

    private static void runJavaAgent(RuntimePodConfiguration configuration,
                                     int maxLoops,
                                     String agentId,
                                     TopicConnectionsRuntime topicConnectionsRuntime,
                                     AgentCode agentCode) throws Exception {
        topicConnectionsRuntime.init(configuration.streamingCluster());

        final TopicConsumer consumer;
        if (configuration.input() != null && !configuration.input().isEmpty()) {
            consumer = topicConnectionsRuntime.createConsumer(agentId,
                    configuration.streamingCluster(), configuration.input());
        } else {
            consumer = new NoopTopicConsumer();
        }

        final TopicProducer producer;
        if (configuration.output() != null && !configuration.output().isEmpty()) {
            producer = topicConnectionsRuntime.createProducer(agentId, configuration.streamingCluster(), configuration.output());
        } else {
            producer = new NoopTopicProducer();
        }


        AgentProcessor mainProcessor;
        if (agentCode instanceof AgentProcessor agentProcessor) {
             mainProcessor = agentProcessor;
        } else {
             mainProcessor = new IdentityAgentProvider.IdentityAgentCode();
        }

        AgentSource source = null;
        if (agentCode instanceof AgentSource agentSource) {
            source = agentSource;
        } else if (agentCode instanceof  CompositeAgentProcessor compositeAgentProcessor) {
            source = compositeAgentProcessor.getSource();
        }
        if (source == null) {
            source = new TopicConsumerSource(consumer);
        }

        AgentSink sink = null;
        if (agentCode instanceof AgentSink agentSink) {
            sink = agentSink;
        } else if (agentCode instanceof CompositeAgentProcessor compositeAgentProcessor) {
            sink = compositeAgentProcessor.getSink();
        }

        if (sink == null) {
            sink = new TopicProducerSink(producer);
        }

        AgentContext agentContext = new SimpleAgentContext(consumer, producer);
        log.info("Source: {}", source);
        log.info("Processor: {}",  mainProcessor);
        log.info("Sink: {}", sink);

        runMainLoop(source,  mainProcessor, sink, agentContext, maxLoops);
    }

    private static void runMainLoop(AgentSource source,
                                    AgentProcessor function,
                                    AgentSink sink,
                                    AgentContext agentContext,
                                    int maxLoops) throws Exception {
        try {
            source.setContext(agentContext);
            sink.setContext(agentContext);
            function.setContext(agentContext);
            source.start();
            sink.start();
            function.start();

            SourceRecordTracker sourceRecordTracker =
                    new SourceRecordTracker(source);
            sink.setCommitCallback(sourceRecordTracker);

            List<Record> records = source.read();
            while ((maxLoops < 0) || (maxLoops-- > 0)) {
                if (records != null && !records.isEmpty()) {
                    try {
                        // the function maps the record coming from the Source to records to be sent to the Sink
                        List<AgentProcessor.SourceRecordAndResult> sinkRecords = function.process(records);

                        sourceRecordTracker.track(sinkRecords);

                        List<Record> forTheSink = new ArrayList<>();
                        sinkRecords.forEach(sourceRecordAndResult -> {
                            sourceRecordAndResult.getResultRecords().forEach(forTheSink::add);
                        });

                        sink.write(forTheSink);
                    } catch (Throwable e) {
                        log.error("Error while processing records", e);

                        // throw the error
                        // this way the consumer will not commit the records
                        throw new RuntimeException("Error while processing records", e);
                    }
                    // commit
                }
                if (sink.handlesCommit()) {
                    // this is the case for the Kafka Connect Sink
                    // in this case it handles directly the Kafka Consumer
                    // and so we bypass the commit
                    sink.commit();
                }
                records = source.read();
            }
        } finally {
            function.close();
            source.close();
            sink.close();
        }
    }

    private static AgentCode initAgent(RuntimePodConfiguration configuration) throws Exception {
        log.info("Bootstrapping agent with configuration {}", configuration.agent());
        return initAgent(configuration.agent().agentType(), configuration.agent().configuration());
    }

    public static AgentCode initAgent(String agentType, Map<String, Object> configuration) throws Exception {
        AgentCode agentCode = AGENT_CODE_REGISTRY.getAgentCode(agentType);
        agentCode.init(configuration);
        return agentCode;
    }

    public static ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    public static void setErrorHandler(ErrorHandler errorHandler) {
        AgentRunner.errorHandler = errorHandler;
    }

    private static class NoopTopicConsumer implements TopicConsumer {
        @SneakyThrows

        @Override
        public List<Record> read() {
            log.info("Sleeping for 1 second, no records...");
            Thread.sleep(1000);
            return List.of();
        }
    }

    private static class NoopTopicProducer implements TopicProducer {
    }

    private static class SimpleAgentContext implements AgentContext {
        private final TopicConsumer consumer;
        private final TopicProducer producer;

        public SimpleAgentContext(TopicConsumer consumer, TopicProducer producer) {
            this.consumer = consumer;
            this.producer = producer;
        }

        @Override
        public TopicConsumer getTopicConsumer() {
            return consumer;
        }

        @Override
        public TopicProducer getTopicProducer() {
            return producer;
        }
    }

}
