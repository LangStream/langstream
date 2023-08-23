/**
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
package ai.langstream.runtime.agent;

import ai.langstream.api.runner.code.AgentCode;
import ai.langstream.api.runner.code.AgentCodeAndLoader;
import ai.langstream.api.runner.code.AgentCodeRegistry;
import ai.langstream.api.runner.code.AgentContext;
import ai.langstream.api.runner.code.AgentProcessor;
import ai.langstream.api.runner.code.AgentSink;
import ai.langstream.api.runner.code.AgentSource;
import ai.langstream.api.runner.code.BadRecordHandler;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.topics.TopicAdmin;
import ai.langstream.api.runner.topics.TopicConnectionsRuntime;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeRegistry;
import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runner.topics.TopicConnectionProvider;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.runtime.agent.api.AgentInfo;
import ai.langstream.runtime.agent.api.AgentInfoServlet;
import ai.langstream.runtime.agent.api.MetricsHttpServlet;
import ai.langstream.runtime.agent.nar.NarFileHandler;
import ai.langstream.runtime.agent.python.PythonCodeAgentProvider;
import ai.langstream.runtime.agent.simple.IdentityAgentProvider;
import ai.langstream.runtime.api.agent.RuntimePodConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.prometheus.client.hotspot.DefaultExports;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static ai.langstream.api.model.ErrorsSpec.DEAD_LETTER;
import static ai.langstream.api.model.ErrorsSpec.FAIL;
import static ai.langstream.api.model.ErrorsSpec.SKIP;

/**
 * This is the main entry point for the pods that run the LangStream runtime and Java code.
 */
@Slf4j
public class AgentRunner
{
    private static final TopicConnectionsRuntimeRegistry TOPIC_CONNECTIONS_REGISTRY = new TopicConnectionsRuntimeRegistry();
    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

    private static MainErrorHandler mainErrorHandler = error -> {
        log.error("Unexpected error", error);
        System.exit(-1);
    };

    public interface MainErrorHandler {
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

            AgentInfo agentInfo = new AgentInfo();
            Server server = bootstrapHttpServer(agentInfo);
            try {
                Path codeDirectory = Path.of(args[1]);
                log.info("Loading code from {}", codeDirectory);

                // handle compatibility with old statefullsets
                Path agentsDirectory = args.length >= 3 ? Path.of(args[2]) : Path.of("/app/agents");
                log.info("Loading agents from {}", codeDirectory);
                run(configuration, podRuntimeConfiguration, codeDirectory, agentsDirectory, agentInfo, -1);
            } finally {
                server.stop();;
            }

        } catch (Throwable error) {
            log.info("Error, NOW SLEEPING", error);
            Thread.sleep(60000);
            mainErrorHandler.handleError(error);
        }
    }

    private static Server bootstrapHttpServer(AgentInfo agentInfo) throws Exception {
        DefaultExports.initialize();
        Server server = new Server(8080);
        log.info("Started metrics and agent server on port 8080");
        String url = "http://" + InetAddress.getLocalHost().getCanonicalHostName() + ":8080";
        log.info("The addresses should be {}/metrics and {}/info}", url);
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        server.setHandler(context);
        context.addServlet(new ServletHolder(new MetricsHttpServlet()), "/metrics");
        context.addServlet(new ServletHolder(new AgentInfoServlet(agentInfo)), "/info");
        server.start();
        return server;
    }


    public static void run(RuntimePodConfiguration configuration,
                           Path podRuntimeConfiguration,
                           Path codeDirectory,
                           Path agentsDirectory,
                           AgentInfo agentInfo,
                           int maxLoops) throws Exception {
        log.info("Pod Configuration {}", configuration);

        // agentId is the identity of the agent in the cluster
        // it is shared by all the instances of the agent
        String agentId = configuration.agent().applicationId() + "-" + configuration.agent().agentId();

        log.info("Starting agent {} with configuration {}", agentId, configuration.agent());

        try (NarFileHandler narFileHandler = new NarFileHandler(agentsDirectory);) {
            narFileHandler.scan();
            ClassLoader customLibClassloader = buildCustomLibClassloader(codeDirectory, Thread.currentThread().getContextClassLoader());
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            try {
                Thread.currentThread().setContextClassLoader(customLibClassloader);

                AgentCodeRegistry agentCodeRegistry = new AgentCodeRegistry();
                agentCodeRegistry.setAgentPackageLoader(narFileHandler);

                TopicConnectionsRuntime topicConnectionsRuntime =
                        TOPIC_CONNECTIONS_REGISTRY.getTopicConnectionsRuntime(configuration.streamingCluster());

                log.info("TopicConnectionsRuntime {}", topicConnectionsRuntime);
                try {
                    AgentCodeAndLoader agentCode = initAgent(configuration, agentCodeRegistry);
                    if (PythonCodeAgentProvider.isPythonCodeAgent(agentCode.agentCode())) {
                        runPythonAgent(
                                podRuntimeConfiguration, codeDirectory);
                    } else {
                        runJavaAgent(configuration, maxLoops, agentId, topicConnectionsRuntime, agentCode, agentInfo);
                    }
                } finally {
                    topicConnectionsRuntime.close();
                }
            } finally {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
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
                "python3", "-m", "langstream_runtime", podRuntimeConfiguration.toAbsolutePath().toString())
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
                                     AgentCodeAndLoader agentCodeWithLoader,
                                     AgentInfo agentInfo) throws Exception {

        topicConnectionsRuntime.init(configuration.streamingCluster());

        // this is closed by the TopicSource
        final TopicConsumer consumer;
        TopicProducer deadLetterProducer = null;
        if (configuration.input() != null && !configuration.input().isEmpty()) {
            consumer = topicConnectionsRuntime.createConsumer(agentId,
                    configuration.streamingCluster(), configuration.input());
            deadLetterProducer = topicConnectionsRuntime.createDeadletterTopicProducer(agentId,
                    configuration.streamingCluster(), configuration.input());
        } else {
            consumer = new NoopTopicConsumer();
        }

        if (deadLetterProducer == null) {
            deadLetterProducer = new NoopTopicProducer();
        }

        // this is closed by the TopicSink
        final TopicProducer producer;
        if (configuration.output() != null && !configuration.output().isEmpty()) {
            producer = topicConnectionsRuntime.createProducer(agentId, configuration.streamingCluster(), configuration.output());
        } else {
            producer = new NoopTopicProducer();
        }

        ErrorsHandler errorsHandler = new StandardErrorsHandler(configuration.agent().errorHandlerConfiguration());

        TopicAdmin topicAdmin = topicConnectionsRuntime.createTopicAdmin(agentId,
                configuration.streamingCluster(),
                configuration.output());

        try {
            AgentProcessor mainProcessor;
            if (agentCodeWithLoader.isProcessor()) {
                mainProcessor = agentCodeWithLoader.asProcessor();
            } else {
                mainProcessor = new IdentityAgentProvider.IdentityAgentCode();
                mainProcessor.setMetadata("identity", "identity", System.currentTimeMillis());
            }
            agentInfo.watchProcessor(mainProcessor);

            AgentSource source = null;
            if (agentCodeWithLoader.isSource()) {
                source = agentCodeWithLoader.asSource();
            } else if (agentCodeWithLoader.is(code -> code instanceof CompositeAgentProcessor)) {
                source = ((CompositeAgentProcessor) agentCodeWithLoader.agentCode()).getSource();
            }

            if (source == null) {
                source = new TopicConsumerSource(consumer, deadLetterProducer);
                source.setMetadata("topic-source", "topic-source", System.currentTimeMillis());
                source.init(Map.of());
            }
            agentInfo.watchSource(source);

            AgentSink sink = null;
            if (agentCodeWithLoader.isSink()) {
                sink = agentCodeWithLoader.asSink();
            } else if (agentCodeWithLoader.is(code -> code instanceof CompositeAgentProcessor)) {
                sink = ((CompositeAgentProcessor) agentCodeWithLoader.agentCode()).getSink();
            }
            if (sink == null) {
                sink = new TopicProducerSink(producer);
                sink.setMetadata("topic-sink", "topic-sink", System.currentTimeMillis());
                sink.init(Map.of());
            }
            agentInfo.watchSink(sink);

            String onBadRecord = configuration.agent().errorHandlerConfiguration()
                    .getOrDefault("onFailure", FAIL).toString();
            final BadRecordHandler brh = getBadRecordHandler(onBadRecord, deadLetterProducer);

            try {
                topicAdmin.start();
                AgentContext agentContext = new SimpleAgentContext(agentId, consumer, producer, topicAdmin, brh,
                        new TopicConnectionProvider() {
                            @Override
                            public TopicConsumer createConsumer(String agentId, Map<String, Object> config) {
                                return topicConnectionsRuntime.createConsumer(agentId, configuration.streamingCluster(), config);
                            }

                            @Override
                            public TopicProducer createProducer(String agentId, Map<String, Object> config) {
                                return topicConnectionsRuntime.createProducer(agentId, configuration.streamingCluster(), config);
                            }
                        });
                log.info("Source: {}", source);
                log.info("Processor: {}", mainProcessor);
                log.info("Sink: {}", sink);

                runMainLoop(source, mainProcessor, sink, agentContext, errorsHandler, maxLoops);
                log.info("Main loop ended");
            } finally {
                mainProcessor.close();
                source.close();
                sink.close();
            }
        } finally {
            topicAdmin.close();
        }
    }

    private static BadRecordHandler getBadRecordHandler(String onBadRecord, final TopicProducer deadLetterProducer) {
        final BadRecordHandler brh;
        if (onBadRecord.equalsIgnoreCase(SKIP)) {
            brh = (record, t, cleanup) -> log.warn("Skipping record {}", record, t);
        } else if(onBadRecord.equalsIgnoreCase(DEAD_LETTER)) {
            brh = (record, t, cleanup) -> {
                log.info("Sending record to dead letter queue {}", record, t);
                deadLetterProducer.write(List.of(record));
            };
        } else {
            brh = (record, t, cleanup) -> {
                cleanup.run();
                if (t instanceof RuntimeException) {
                    throw (RuntimeException) t;
                }
                throw new RuntimeException(t);
            };
        }
        return brh;
    }

    static void runMainLoop(AgentSource source,
                                    AgentProcessor function,
                                    AgentSink sink,
                                    AgentContext agentContext,
                                    ErrorsHandler errorsHandler,
                                    int maxLoops) throws Exception {
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
                // in case of permanent FAIL this method will throw an exception
                List<AgentProcessor.SourceRecordAndResult> sinkRecords
                        = runProcessorAgent(function, records, errorsHandler, source);
                // sinkRecord == null is the SKIP case

                // in this case we do not send the records to the sink
                // and the source has already committed the records
                // This won't for the Kafka Connect Sink, because it does handle the commit
                // itself, but on the other hand in the case of the Connect Sink we can see here
                // only the IdentityAgentCode that is not supposed to fail
                try {
                    // the function maps the record coming from the Source to records to be sent to the Sink
                    sourceRecordTracker.track(sinkRecords);
                    for (AgentProcessor.SourceRecordAndResult sourceRecordAndResult : sinkRecords) {
                        List<Record> forTheSink = new ArrayList<>(sourceRecordAndResult.getResultRecords());
                        sink.write(forTheSink);
                    }
                } catch (Throwable e) {
                    log.error("Error while processing records", e);

                    // throw the error
                    // this way the consumer will not commit the records
                    throw new RuntimeException("Error while processing records", e);
                }

            }

            // commit (Kafka Connect Sink)
            if (sink.handlesCommit()) {
                // this is the case for the Kafka Connect Sink
                // in this case it handles directly the Kafka Consumer
                // and so we bypass the commit
                sink.commit();
            }
            records = source.read();
        }
    }


    private static List<AgentProcessor.SourceRecordAndResult> runProcessorAgent(AgentProcessor processor,
                                                                                List<Record> sourceRecords,
                                                                                ErrorsHandler errorsHandler,
                                                                                AgentSource source) throws Exception {
        List<Record> recordToProcess = sourceRecords;
        Map<Record, AgentProcessor.SourceRecordAndResult> resultsByRecord = new HashMap<>();
        int trialNumber = 0;
        while (!recordToProcess.isEmpty()) {
            log.info("runProcessor on {} records (trial #{})", recordToProcess.size(), trialNumber++);
            List<AgentProcessor.SourceRecordAndResult> results = safeProcessRecords(processor, recordToProcess);

            recordToProcess = new ArrayList<>();
            for (AgentProcessor.SourceRecordAndResult result : results) {
                Record sourceRecord = result.getSourceRecord();
                log.info("Result for record {}: {}", sourceRecord, result);
                resultsByRecord.put(sourceRecord, result);
                if (result.getError() != null) {
                    Throwable error = result.getError();
                    // handle error
                    ErrorsHandler.ErrorsProcessingOutcome action = errorsHandler.handleErrors(sourceRecord, result.getError());
                    switch (action) {
                        case SKIP:
                            log.error("Unrecoverable error while processing the records, skipping", error);
                            resultsByRecord.put(sourceRecord,
                                    new AgentProcessor.SourceRecordAndResult(sourceRecord, List.of(), error));
                            break;
                        case RETRY:
                            log.error("Retryable error while processing the records, retrying", error);
                            // retry
                            recordToProcess.add(sourceRecord);
                            break;
                        case FAIL:
                            log.error("Unrecoverable error while processing some the records, failing", error);
                            PermanentFailureException permanentFailureException = new PermanentFailureException(error);
                            source.permanentFailure(sourceRecord, permanentFailureException);
                            if (errorsHandler.failProcessingOnPermanentErrors()) {
                                log.error("Failing processing on permanent error");
                                throw permanentFailureException;
                            }
                            // in case the source does not throw an exception we mark the record as "skipped"
                            resultsByRecord.put(sourceRecord,
                                    new AgentProcessor.SourceRecordAndResult(sourceRecord, List.of(), error));
                            break;
                        default:
                            throw new IllegalStateException("Unexpected value: " + action);
                    }
                }
            }
        }

        List<AgentProcessor.SourceRecordAndResult> finalResult = new ArrayList<>(sourceRecords.size());
        for (Record sourceRecord : sourceRecords) {
            AgentProcessor.SourceRecordAndResult sourceRecordAndResult = resultsByRecord.get(sourceRecord);
            log.info("final {} with {} sink records", sourceRecord, sourceRecordAndResult.getResultRecords().size());
            finalResult.add(sourceRecordAndResult);
        }
        return finalResult;
    }

    private static List<AgentProcessor.SourceRecordAndResult> safeProcessRecords(AgentProcessor processor, List<Record> recordToProcess) {
        List<AgentProcessor.SourceRecordAndResult> results;
        try {
            results =  processor.process(recordToProcess);
        } catch (Throwable error) {
            results = recordToProcess
                    .stream()
                    .map(r -> new AgentProcessor.SourceRecordAndResult(r, null, error))
                    .toList();
        }
        return results;
    }

    public static final class PermanentFailureException extends Exception {
        public PermanentFailureException(Throwable cause) {
            super(cause);
        }
    }

    private static AgentCodeAndLoader initAgent(RuntimePodConfiguration configuration, AgentCodeRegistry agentCodeRegistry) throws Exception {
        log.info("Bootstrapping agent with configuration {}", configuration.agent());
        return initAgent(configuration.agent().agentId(), configuration.agent().agentType(),
                System.currentTimeMillis(),
                configuration.agent().configuration(),
                agentCodeRegistry);
    }

    public static AgentCodeAndLoader initAgent(String agentId, String agentType, long startedAt, Map<String, Object> configuration,
                                               AgentCodeRegistry agentCodeRegistry) throws Exception {
        AgentCodeAndLoader agentCodeAndLoader = agentCodeRegistry.getAgentCode(agentType);
        agentCodeAndLoader.executeWithContextClassloader((AgentCode agentCode) ->  {
                 if (agentCode instanceof CompositeAgentProcessor compositeAgentProcessor) {
                     compositeAgentProcessor.configureAgentCodeRegistry(agentCodeRegistry);
                 }

                 agentCode.setMetadata(agentId, agentType, startedAt);
                 agentCode.init(configuration);
         });
        return agentCodeAndLoader;
    }

    public static MainErrorHandler getErrorHandler() {
        return mainErrorHandler;
    }

    public static void setErrorHandler(MainErrorHandler mainErrorHandler) {
        AgentRunner.mainErrorHandler = mainErrorHandler;
    }

    private static class NoopTopicConsumer implements TopicConsumer {
        @SneakyThrows

        @Override
        public List<Record> read() {
            log.info("Sleeping for 1 second, no records...");
            Thread.sleep(1000);
            return List.of();
        }

        @Override
        public long getTotalOut() {
            return 0;
        }
    }

    private static class NoopTopicProducer implements TopicProducer {
        @Override
        public long getTotalIn() {
            return 0;
        }
    }

    private static class SimpleAgentContext implements AgentContext {
        private final TopicConsumer consumer;
        private final TopicProducer producer;
        private final TopicAdmin topicAdmin;
        private final String agentId;

        private final TopicConnectionProvider topicConnectionProvider;
        private final BadRecordHandler brh;

        public SimpleAgentContext(String agentId, TopicConsumer consumer, TopicProducer producer, TopicAdmin topicAdmin,
                                  BadRecordHandler brh,
                                  TopicConnectionProvider topicConnectionProvider) {
            this.consumer = consumer;
            this.producer = producer;
            this.topicAdmin = topicAdmin;
            this.agentId = agentId;
            this.brh = brh;
            this.topicConnectionProvider = topicConnectionProvider;
        }

        @Override
        public String getGlobalAgentId() {
            return agentId;
        }

        @Override
        public TopicConsumer getTopicConsumer() {
            return consumer;
        }

        @Override
        public TopicProducer getTopicProducer() {
            return producer;
        }

        @Override
        public TopicAdmin getTopicAdmin() {
            return topicAdmin;
        }

        @Override
        public BadRecordHandler getBadRecordHandler() {
            return brh;
        }

        @Override
        public TopicConnectionProvider getTopicConnectionProvider() {
            return topicConnectionProvider;
        }
    }

}
