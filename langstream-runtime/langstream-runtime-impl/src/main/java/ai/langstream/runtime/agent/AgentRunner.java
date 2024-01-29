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
package ai.langstream.runtime.agent;

import static ai.langstream.api.model.ErrorsSpec.DEAD_LETTER;
import static ai.langstream.api.model.ErrorsSpec.FAIL;
import static ai.langstream.api.model.ErrorsSpec.SKIP;

import ai.langstream.api.runner.code.AgentCode;
import ai.langstream.api.runner.code.AgentCodeAndLoader;
import ai.langstream.api.runner.code.AgentCodeRegistry;
import ai.langstream.api.runner.code.AgentContext;
import ai.langstream.api.runner.code.AgentProcessor;
import ai.langstream.api.runner.code.AgentService;
import ai.langstream.api.runner.code.AgentSink;
import ai.langstream.api.runner.code.AgentSource;
import ai.langstream.api.runner.code.AgentStatusResponse;
import ai.langstream.api.runner.code.BadRecordHandler;
import ai.langstream.api.runner.code.MetricsReporter;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.RecordSink;
import ai.langstream.api.runner.topics.TopicAdmin;
import ai.langstream.api.runner.topics.TopicConnectionProvider;
import ai.langstream.api.runner.topics.TopicConnectionsRuntime;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeAndLoader;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeRegistry;
import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.api.runtime.ComponentType;
import ai.langstream.impl.nar.NarFileHandler;
import ai.langstream.runtime.agent.api.AgentAPIController;
import ai.langstream.runtime.agent.api.AgentInfoServlet;
import ai.langstream.runtime.agent.api.MetricsHttpServlet;
import ai.langstream.runtime.agent.simple.IdentityAgentProvider;
import ai.langstream.runtime.api.agent.RuntimePodConfiguration;
import io.prometheus.client.hotspot.DefaultExports;
import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

/** This is the main entry point for the pods that run the LangStream runtime and Java code. */
@Slf4j
public class AgentRunner {
    private static MainErrorHandler mainErrorHandler =
            error -> {
                log.error("Unexpected error", error);
                System.exit(-1);
            };

    public interface MainErrorHandler {
        void handleError(Throwable error);
    }

    private static Server bootstrapHttpServer(AgentAPIController agentAPIController)
            throws Exception {
        DefaultExports.initialize();
        Server server = new Server(8080);
        log.info("Started metrics and agent server on port 8080");
        String url = "http://" + InetAddress.getLocalHost().getCanonicalHostName() + ":8080";
        log.info("The addresses should be {}/metrics and {}/info}", url, url);
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        server.setHandler(context);
        context.addServlet(new ServletHolder(new MetricsHttpServlet()), "/metrics");
        context.addServlet(new ServletHolder(new AgentInfoServlet(agentAPIController)), "/info");
        server.start();
        return server;
    }

    public static void runAgent(
            RuntimePodConfiguration configuration,
            Path codeDirectory,
            Path agentsDirectory,
            Path basePersistentStateDirectory,
            AgentAPIController agentAPIController,
            Supplier<Boolean> continueLoop,
            Runnable beforeStopSource,
            boolean startHttpServer,
            NarFileHandler narFileHandler,
            MetricsReporter metricsReporter)
            throws Exception {
        new AgentRunner()
                .run(
                        configuration,
                        codeDirectory,
                        agentsDirectory,
                        basePersistentStateDirectory,
                        agentAPIController,
                        continueLoop,
                        beforeStopSource,
                        startHttpServer,
                        narFileHandler,
                        metricsReporter);
    }

    public void run(
            RuntimePodConfiguration configuration,
            Path codeDirectory,
            Path agentsDirectory,
            Path basePersistentStateDirectory,
            AgentAPIController agentAPIController,
            Supplier<Boolean> continueLoop,
            Runnable beforeStopSource,
            boolean startHttpServer,
            NarFileHandler sharedNarFileHandler,
            MetricsReporter metricsReporter)
            throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("Pod Configuration {}", configuration);
        }

        // agentId is the identity of the agent in the cluster
        // it is shared by all the instances of the agent
        String agentId =
                configuration.agent().applicationId() + "-" + configuration.agent().agentId();

        log.info("Starting agent {}", agentId);
        log.info("Code directory {}", codeDirectory);
        log.info("Base persistent state directory {}", basePersistentStateDirectory);

        boolean sharingNarFileHandler = sharedNarFileHandler != null;
        List<URL> customLibClasspath = buildCustomLibClasspath(codeDirectory);
        NarFileHandler narFileHandler =
                sharingNarFileHandler
                        ? sharedNarFileHandler
                        : new NarFileHandler(
                                agentsDirectory,
                                customLibClasspath,
                                Thread.currentThread().getContextClassLoader());
        try {
            if (!sharingNarFileHandler) {
                narFileHandler.scan();
            }

            TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry =
                    new TopicConnectionsRuntimeRegistry();
            topicConnectionsRuntimeRegistry.setPackageLoader(narFileHandler);

            AgentCodeRegistry agentCodeRegistry = new AgentCodeRegistry();
            agentCodeRegistry.setAgentPackageLoader(narFileHandler);

            TopicConnectionsRuntimeAndLoader topicConnectionsRuntimeWithClassloader =
                    topicConnectionsRuntimeRegistry.getTopicConnectionsRuntime(
                            configuration.streamingCluster());
            try {
                TopicConnectionsRuntime topicConnectionsRuntime =
                        topicConnectionsRuntimeWithClassloader.asTopicConnectionsRuntime();
                AgentCodeAndLoader agentCode = initAgent(configuration, agentCodeRegistry);
                Server server = null;
                try {
                    server = startHttpServer ? bootstrapHttpServer(agentAPIController) : null;
                    runJavaAgent(
                            configuration,
                            continueLoop,
                            agentId,
                            topicConnectionsRuntime,
                            agentCode,
                            agentAPIController,
                            beforeStopSource,
                            codeDirectory,
                            basePersistentStateDirectory,
                            metricsReporter);
                } finally {
                    if (server != null) {
                        server.stop();
                    }
                }
            } finally {
                topicConnectionsRuntimeWithClassloader.close();
            }
        } finally {
            if (!sharingNarFileHandler) {
                narFileHandler.close();
            }
        }
    }

    public static List<URL> buildCustomLibClasspath(Path codeDirectory) throws IOException {
        if (codeDirectory == null) {
            return List.of();
        }
        Path javaLib = codeDirectory.resolve("java").resolve("lib");
        log.info("Looking for java lib in {}", javaLib);
        if (Files.exists(javaLib) && Files.isDirectory(javaLib)) {
            List<URL> jars;
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(javaLib, "*.jar")) {
                jars =
                        StreamSupport.stream(stream.spliterator(), false)
                                .map(
                                        p -> {
                                            log.info("Adding jar {}", p);
                                            try {
                                                return p.toUri().toURL();
                                            } catch (MalformedURLException e) {
                                                log.error("Error adding jar {}", p, e);
                                                // ignore
                                                return null;
                                            }
                                        })
                                .filter(Objects::nonNull)
                                .collect(Collectors.toList());
            }
            return jars;
        }
        return List.of();
    }

    private static void runJavaAgent(
            RuntimePodConfiguration configuration,
            Supplier<Boolean> continueLoop,
            String agentId,
            TopicConnectionsRuntime topicConnectionsRuntime,
            AgentCodeAndLoader agentCodeWithLoader,
            AgentAPIController agentAPIController,
            Runnable beforeStopSource,
            Path codeDirectory,
            Path basePersistentStateDirectory,
            MetricsReporter metricsReporter)
            throws Exception {

        Set<String> agentsWithPersistentState = configuration.agent().agentsWithDisk();
        if (agentsWithPersistentState == null) {
            agentsWithPersistentState = Set.of();
        }

        String statsThreadName = "stats-" + configuration.agent().agentId();
        ScheduledExecutorService statsScheduler =
                Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, statsThreadName));
        try {

            topicConnectionsRuntime.init(configuration.streamingCluster());

            // this is closed by the TopicSource
            final TopicConsumer consumer;
            TopicProducer deadLetterProducer = null;
            if (configuration.input() != null && !configuration.input().isEmpty()) {
                consumer =
                        topicConnectionsRuntime.createConsumer(
                                agentId, configuration.streamingCluster(), configuration.input());
                deadLetterProducer =
                        topicConnectionsRuntime.createDeadletterTopicProducer(
                                agentId, configuration.streamingCluster(), configuration.input());
            } else {
                consumer = new NoopTopicConsumer();
            }

            if (deadLetterProducer == null) {
                deadLetterProducer = new NoopTopicProducer();
            }

            // this is closed by the TopicSink
            final TopicProducer producer;
            if (configuration.output() != null && !configuration.output().isEmpty()) {
                producer =
                        topicConnectionsRuntime.createProducer(
                                agentId, configuration.streamingCluster(), configuration.output());
            } else {
                producer = new NoopTopicProducer();
            }

            ErrorsHandler errorsHandler =
                    new StandardErrorsHandler(configuration.agent().errorHandlerConfiguration());

            try (TopicAdmin topicAdmin =
                    topicConnectionsRuntime.createTopicAdmin(
                            agentId, configuration.streamingCluster(), configuration.output())) {

                AgentService mainService = null;
                AgentProcessor mainProcessor = null;
                AgentSource source = null;
                AgentSink sink = null;

                if (agentCodeWithLoader.isService()) {
                    mainService = agentCodeWithLoader.asService();
                    agentAPIController.watchService(mainService);
                } else {
                    if (agentCodeWithLoader.isProcessor()) {
                        mainProcessor = agentCodeWithLoader.asProcessor();
                    } else {
                        mainProcessor = new IdentityAgentProvider.IdentityAgentCode();
                        mainProcessor.setMetadata(
                                "identity", "identity", System.currentTimeMillis());
                    }
                    agentAPIController.watchProcessor(mainProcessor);

                    if (agentCodeWithLoader.isSource()) {
                        source = agentCodeWithLoader.asSource();
                    } else if (agentCodeWithLoader.is(
                            code -> code instanceof CompositeAgentProcessor)) {
                        source =
                                ((CompositeAgentProcessor) agentCodeWithLoader.agentCode())
                                        .getSource();
                    }

                    if (source == null) {
                        source = new TopicConsumerSource(consumer, deadLetterProducer);
                        source.setMetadata(
                                "topic-source", "topic-source", System.currentTimeMillis());
                        source.init(Map.of());
                    }
                    agentAPIController.watchSource(source);

                    if (agentCodeWithLoader.isSink()) {
                        sink = agentCodeWithLoader.asSink();
                    } else if (agentCodeWithLoader.is(
                            code -> code instanceof CompositeAgentProcessor)) {
                        sink =
                                ((CompositeAgentProcessor) agentCodeWithLoader.agentCode())
                                        .getSink();
                    }
                    if (sink == null) {
                        sink = new TopicProducerSink(producer);
                        sink.setMetadata("topic-sink", "topic-sink", System.currentTimeMillis());
                        sink.init(Map.of());
                    }
                    agentAPIController.watchSink(sink);
                }

                String onBadRecord =
                        configuration
                                .agent()
                                .errorHandlerConfiguration()
                                .getOrDefault("onFailure", FAIL)
                                .toString();
                final BadRecordHandler brh = getBadRecordHandler(onBadRecord, deadLetterProducer);

                try {
                    topicAdmin.start();
                    AgentContext agentContext =
                            new SimpleAgentContext(
                                    agentId,
                                    consumer,
                                    producer,
                                    topicAdmin,
                                    brh,
                                    new TopicConnectionProvider() {
                                        @Override
                                        public TopicConsumer createConsumer(
                                                String agentId, Map<String, Object> config) {
                                            return topicConnectionsRuntime.createConsumer(
                                                    agentId,
                                                    configuration.streamingCluster(),
                                                    config);
                                        }

                                        @Override
                                        public TopicProducer createProducer(
                                                String agentId,
                                                String topic,
                                                Map<String, Object> config) {
                                            if (topic != null && !topic.isEmpty()) {
                                                if (config == null) {
                                                    config = Map.of("topic", topic);
                                                } else {
                                                    config = new HashMap<>(config);
                                                    config.put("topic", topic);
                                                }
                                            }
                                            return topicConnectionsRuntime.createProducer(
                                                    agentId,
                                                    configuration.streamingCluster(),
                                                    config);
                                        }
                                    },
                                    codeDirectory,
                                    basePersistentStateDirectory,
                                    agentsWithPersistentState,
                                    metricsReporter);
                    log.info("Source: {}", source);
                    log.info("Processor: {}", mainProcessor);
                    log.info("Sink: {}", sink);
                    log.info("Service: {}", mainService);

                    if (mainService != null) {
                        mainService.setContext(agentContext);
                        mainService.start();
                        log.info("Service started");
                        mainService.join();
                        log.info("Service ended");
                    } else {
                        PendingRecordsCounterSource pendingRecordsCounterSource =
                                new PendingRecordsCounterSource(source, sink.handlesCommit());

                        statsScheduler.scheduleAtFixedRate(
                                pendingRecordsCounterSource::dumpStats, 30, 30, TimeUnit.SECONDS);

                        runMainLoop(
                                pendingRecordsCounterSource,
                                mainProcessor,
                                sink,
                                agentContext,
                                errorsHandler,
                                continueLoop);

                        pendingRecordsCounterSource.waitForNoPendingRecords();
                    }

                    log.info("Main loop ended");

                } finally {
                    if (mainProcessor != null) {
                        mainProcessor.close();
                    }

                    if (beforeStopSource != null) {
                        // we want to perform validations after stopping the processors
                        // but without stopping the source (otherwise we cannot see the status of
                        // the
                        // consumers)
                        beforeStopSource.run();
                    }

                    if (source != null) {
                        source.close();
                    }

                    if (sink != null) {
                        sink.close();
                    }

                    if (mainService != null) {
                        mainService.close();
                    }

                    log.info("Agent fully stopped");
                }
            }
        } finally {
            statsScheduler.shutdown();
        }
    }

    private static final class PendingRecordsCounterSource implements AgentSource {
        private final AgentSource wrapped;
        private final Set<Record> pendingRecords = ConcurrentHashMap.newKeySet();
        private final AtomicLong totalSourceRecords = new AtomicLong();
        private final boolean sinkHandlesCommits;

        public PendingRecordsCounterSource(AgentSource wrapped, boolean sinkHandlesCommits) {
            this.wrapped = wrapped;
            this.sinkHandlesCommits = sinkHandlesCommits;
        }

        @Override
        public String agentId() {
            return wrapped.agentId();
        }

        @Override
        public String agentType() {
            return wrapped.agentType();
        }

        @Override
        public void setMetadata(String id, String agentType, long startedAt) throws Exception {
            wrapped.setMetadata(id, agentType, startedAt);
        }

        @Override
        public void init(Map<String, Object> configuration) throws Exception {
            wrapped.init(configuration);
        }

        @Override
        public void setContext(AgentContext context) throws Exception {
            wrapped.setContext(context);
        }

        @Override
        public void start() throws Exception {
            wrapped.start();
        }

        @Override
        public void close() throws Exception {
            wrapped.close();
        }

        @Override
        public List<AgentStatusResponse> getAgentStatus() {
            return wrapped.getAgentStatus();
        }

        @Override
        public List<Record> read() throws Exception {
            List<Record> read = wrapped.read();
            if (read != null) {
                totalSourceRecords.addAndGet(read.size());
                if (!sinkHandlesCommits) {
                    // is the Sink handles the commit (Kafka Connect case)
                    // then it doesn't notify the Source of the commit,
                    // so we cannot track this here, otherwise it is a memory leak
                    pendingRecords.addAll(read);
                }
            }
            return read;
        }

        @Override
        public void commit(List<Record> records) throws Exception {
            pendingRecords.removeAll(records);
            wrapped.commit(records);
        }

        @Override
        public ComponentType componentType() {
            return wrapped.componentType();
        }

        @Override
        public void permanentFailure(Record record, Exception error) throws Exception {
            wrapped.permanentFailure(record, error);
        }

        @Override
        public String toString() {
            return wrapped.toString();
        }

        public void waitForNoPendingRecords() {
            long start = System.currentTimeMillis();
            try {
                while (!pendingRecords.isEmpty()) {
                    int size = pendingRecords.size();
                    if (size <= 10) {
                        log.info(
                                "Waiting for {} pending records: {}",
                                pendingRecords.size(),
                                pendingRecords);
                    } else {
                        Record first = null;
                        try {
                            first = pendingRecords.iterator().next();
                        } catch (NoSuchElementException e) {
                            // ignore
                        }
                        log.info(
                                "Waiting for {} pending records, one of them is {}",
                                pendingRecords.size(),
                                first);
                    }

                    long now = System.currentTimeMillis();
                    long delta = now - start;
                    if (delta > 60000) {
                        log.error(
                                "Waited for {} pending records for more than 60 seconds, existing anyway",
                                pendingRecords.size());
                        return;
                    }

                    Thread.sleep(1000);
                }
            } catch (InterruptedException interruptedException) {
                // exist the loop on IE
                Thread.currentThread().interrupt();
            }
        }

        static final int MB = 1024 * 1024;

        public void dumpStats() {
            Object currentRecords = sinkHandlesCommits ? "N/A" : pendingRecords.size();
            Runtime instance = Runtime.getRuntime();

            BufferPoolMXBean directMemory =
                    ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class).stream()
                            .filter(b -> b.getName().contains("direct"))
                            .findFirst()
                            .orElse(null);

            log.info(
                    "Records: total {}, working {}, Memory stats: used {} MB, total {} MB, free {} MB, max {} MB "
                            + "Direct memory {} MB",
                    totalSourceRecords.get(),
                    currentRecords,
                    (instance.totalMemory() - instance.freeMemory()) / MB,
                    instance.totalMemory() / MB,
                    instance.freeMemory() / MB,
                    instance.maxMemory() / MB,
                    directMemory != null ? directMemory.getMemoryUsed() / MB : "?");
        }
    }

    private static BadRecordHandler getBadRecordHandler(
            String onBadRecord, final TopicProducer deadLetterProducer) {
        final BadRecordHandler brh;
        if (onBadRecord.equalsIgnoreCase(SKIP)) {
            brh = (record, t, cleanup) -> log.warn("Skipping record {}", record, t);
        } else if (onBadRecord.equalsIgnoreCase(DEAD_LETTER)) {
            brh =
                    (record, t, cleanup) -> {
                        log.info("Sending record to dead letter queue {}", record, t);
                        deadLetterProducer.write(record).join();
                    };
        } else {
            brh =
                    (record, t, cleanup) -> {
                        cleanup.run();
                        if (t instanceof RuntimeException) {
                            throw (RuntimeException) t;
                        }
                        throw new RuntimeException(t);
                    };
        }
        return brh;
    }

    static void runMainLoop(
            AgentSource source,
            AgentProcessor processor,
            AgentSink sink,
            AgentContext agentContext,
            ErrorsHandler errorsHandler,
            Supplier<Boolean> continueLoop)
            throws Exception {
        source.setContext(agentContext);
        sink.setContext(agentContext);
        processor.setContext(agentContext);
        source.start();
        sink.start();
        processor.start();

        SourceRecordTracker sourceRecordTracker = new SourceRecordTracker(source);
        AtomicReference<Exception> fatalError = new AtomicReference<>();

        while (continueLoop.get()) {
            List<Record> records = source.read();
            if (records != null && !records.isEmpty()) {
                // in case of permanent FAIL this method will throw an exception
                runProcessorAgent(
                        processor,
                        records,
                        errorsHandler,
                        source,
                        (AgentProcessor.SourceRecordAndResult sourceRecordAndResult) -> {
                            if (sourceRecordAndResult.error() != null) {
                                log.error("Fatal error", sourceRecordAndResult.error());
                                // handle error
                                setFatalError(sourceRecordAndResult.error(), fatalError);
                                return;
                            }

                            if (sourceRecordAndResult.resultRecords().isEmpty()) {
                                if (log.isDebugEnabled()) {
                                    log.debug(
                                            "No records to send to the Sink for {}",
                                            sourceRecordAndResult.sourceRecord());
                                }
                                // no records, we have to commit the source record to the source
                                // no need to call the Sink with an empty list
                                try {
                                    source.commit(List.of(sourceRecordAndResult.sourceRecord()));
                                } catch (Throwable error) {
                                    log.error("Source could not commit the record", error);
                                    setFatalError(error, fatalError);
                                }
                                return;
                            }

                            sourceRecordTracker.track(List.of(sourceRecordAndResult));
                            try {
                                // the processor maps the record coming from the Source to records
                                // to be sent to the Sink
                                processRecordsOnTheSink(
                                        sink,
                                        sourceRecordAndResult,
                                        errorsHandler,
                                        sourceRecordTracker,
                                        source,
                                        fatalError);
                            } catch (Throwable e) {
                                log.error("Error while processing records", e);
                                setFatalError(e, fatalError);
                            }
                        });
            }
            checkFatalError(fatalError);

            // commit (Kafka Connect Sink)
            if (sink.handlesCommit()) {
                // this is the case for the Kafka Connect Sink
                // in this case it handles directly the Kafka Consumer
                // and so we bypass the commit
                sink.commit();
            }
        }
    }

    private static void checkFatalError(AtomicReference<Exception> fatalError) throws Exception {
        if (fatalError.get() != null) {
            throw fatalError.get();
        }
    }

    private static void setFatalError(Throwable e, AtomicReference<Exception> fatalError) {
        Exception value;
        if (e instanceof PermanentFailureException pf) {
            value = pf;
        } else {
            // throw the error
            // this way the consumer will not commit the records
            value = new RuntimeException("Error while processing records", e);
        }
        fatalError.compareAndSet(null, value);
    }

    private static void processRecordsOnTheSink(
            AgentSink sink,
            AgentProcessor.SourceRecordAndResult sourceRecordAndResult,
            ErrorsHandler errorsHandler,
            SourceRecordTracker sourceRecordTracker,
            AgentSource source,
            AtomicReference<Exception> fatalError) {
        Record sourceRecord = sourceRecordAndResult.sourceRecord();
        List<Record> toWrite = new ArrayList<>(sourceRecordAndResult.resultRecords());
        for (Record record : toWrite) {
            writeRecordToTheSink(
                    sink,
                    errorsHandler,
                    sourceRecordTracker,
                    source,
                    fatalError,
                    sourceRecord,
                    record);
        }
    }

    private static void writeRecordToTheSink(
            AgentSink sink,
            ErrorsHandler errorsHandler,
            SourceRecordTracker sourceRecordTracker,
            AgentSource source,
            AtomicReference<Exception> fatalError,
            Record sourceRecord,
            Record record) {
        CompletableFuture<?> writeResult = sink.write(record);

        if (sink.handlesCommit()) {
            // it is the sink that handles the commit
            // we should not commit the source record or handle failures
            writeResult.exceptionally(
                    error -> {
                        log.error(
                                "Error while writing record {} on a Sink that handles commits by itself",
                                record,
                                error);
                        setFatalError(error, fatalError);
                        return null;
                    });
            return;
        }

        writeResult.whenComplete(
                (___, error) -> {
                    if (error == null) {
                        sourceRecordTracker.commit(List.of(record));
                    } else {
                        // handle error
                        ErrorsHandler.ErrorsProcessingOutcome action =
                                errorsHandler.handleErrors(sourceRecord, error);
                        switch (action) {
                            case SKIP -> {
                                // skip (the whole batch)
                                log.error(
                                        "Unrecoverable error while processing the records, skipping",
                                        error);
                                sourceRecordTracker.commit(List.of(record));
                            }
                            case RETRY -> {
                                log.error(
                                        "Retryable error while processing the records, retrying",
                                        error);
                                writeRecordToTheSink(
                                        sink,
                                        errorsHandler,
                                        sourceRecordTracker,
                                        source,
                                        fatalError,
                                        sourceRecord,
                                        record);
                            }
                            case FAIL -> {
                                log.error(
                                        "Unrecoverable error while processing some the records, failing",
                                        error);
                                PermanentFailureException permanentFailureException =
                                        new PermanentFailureException(error);
                                try {
                                    source.permanentFailure(
                                            sourceRecord, permanentFailureException);
                                } catch (Exception err) {
                                    err.addSuppressed(permanentFailureException);
                                    log.error("Cannot send permanent failure to the source", err);
                                    setFatalError(err, fatalError);
                                }
                                if (errorsHandler.failProcessingOnPermanentErrors()) {
                                    log.error("Failing processing on permanent error");
                                    setFatalError(permanentFailureException, fatalError);
                                } else {
                                    // in case the source does not throw an exception we mark the
                                    // record as "skipped"
                                    sourceRecordTracker.commit(List.of(record));
                                }
                                return;
                            }
                            default -> throw new IllegalStateException(
                                    "Unexpected value: " + action);
                        }
                    }
                });
    }

    private static void runProcessorAgent(
            AgentProcessor processor,
            List<Record> sourceRecords,
            ErrorsHandler errorsHandler,
            AgentSource source,
            RecordSink finalSink) {
        if (log.isDebugEnabled()) {
            log.debug("runProcessor on {} records", sourceRecords.size());
        }
        processor.process(
                sourceRecords,
                (AgentProcessor.SourceRecordAndResult result) -> {
                    Record sourceRecord = result.sourceRecord();
                    try {
                        if (result.error() != null) {
                            Throwable error = result.error();
                            // handle error
                            ErrorsHandler.ErrorsProcessingOutcome action =
                                    errorsHandler.handleErrors(sourceRecord, result.error());
                            switch (action) {
                                case SKIP -> {
                                    log.error(
                                            "Unrecoverable error while processing the records, skipping",
                                            error);
                                    finalSink.emit(
                                            new AgentProcessor.SourceRecordAndResult(
                                                    sourceRecord, List.of(), null));
                                }
                                case RETRY -> {
                                    log.error(
                                            "Retryable error while processing the records, retrying",
                                            error);
                                    // retry the single record (this leads to out-of-order
                                    // processing)
                                    runProcessorAgent(
                                            processor,
                                            List.of(sourceRecord),
                                            errorsHandler,
                                            source,
                                            finalSink);
                                }
                                case FAIL -> {
                                    log.error(
                                            "Unrecoverable error while processing some the records, failing",
                                            error);
                                    PermanentFailureException permanentFailureException =
                                            new PermanentFailureException(error);
                                    permanentFailureException.fillInStackTrace();
                                    source.permanentFailure(
                                            sourceRecord, permanentFailureException);
                                    if (errorsHandler.failProcessingOnPermanentErrors()) {
                                        log.error("Failing processing on permanent error");
                                        finalSink.emit(
                                                new AgentProcessor.SourceRecordAndResult(
                                                        sourceRecord,
                                                        List.of(),
                                                        permanentFailureException));
                                    } else {
                                        // in case the source does not throw an exception we mark
                                        // the record as "skipped"
                                        finalSink.emit(
                                                new AgentProcessor.SourceRecordAndResult(
                                                        sourceRecord, List.of(), null));
                                    }
                                }
                                default -> {
                                    finalSink.emit(
                                            new AgentProcessor.SourceRecordAndResult(
                                                    sourceRecord,
                                                    List.of(),
                                                    new PermanentFailureException(
                                                            new IllegalStateException())));
                                }
                            }
                        } else {
                            if (log.isDebugEnabled()) {
                                log.debug("Passing {} to the Sink", result);
                            }
                            finalSink.emit(result);
                        }
                    } catch (Throwable error) {
                        log.error("Error while processing record {}", sourceRecord, error);
                        finalSink.emit(
                                new AgentProcessor.SourceRecordAndResult(
                                        sourceRecord, List.of(), error));
                    }
                });
    }

    public static final class PermanentFailureException extends Exception {
        public PermanentFailureException(Throwable cause) {
            super(cause);
        }
    }

    private static AgentCodeAndLoader initAgent(
            RuntimePodConfiguration configuration, AgentCodeRegistry agentCodeRegistry)
            throws Exception {
        log.info(
                "Bootstrapping agent {} type {}",
                configuration.agent().agentId(),
                configuration.agent().agentType());
        return initAgent(
                configuration.agent().agentId(),
                configuration.agent().agentType(),
                System.currentTimeMillis(),
                configuration.agent().configuration(),
                agentCodeRegistry);
    }

    public static AgentCodeAndLoader initAgent(
            String agentId,
            String agentType,
            long startedAt,
            Map<String, Object> configuration,
            AgentCodeRegistry agentCodeRegistry)
            throws Exception {
        AgentCodeAndLoader agentCodeAndLoader = agentCodeRegistry.getAgentCode(agentType);
        agentCodeAndLoader.executeWithContextClassloader(
                (AgentCode agentCode) -> {
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
        private final AtomicLong totalIn = new AtomicLong();

        @Override
        public CompletableFuture<?> write(Record record) {
            totalIn.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public long getTotalIn() {
            return totalIn.get();
        }
    }

    private static class SimpleAgentContext implements AgentContext {
        private final TopicConsumer consumer;
        private final TopicProducer producer;
        private final TopicAdmin topicAdmin;
        private final String globalAgentId;

        private final TopicConnectionProvider topicConnectionProvider;
        private final BadRecordHandler brh;

        private final Path codeDirectory;
        private final Path basePersistentStateDirectory;
        private final MetricsReporter metricsReporter;

        private final Set<String> agentsWithPersistentState;

        public SimpleAgentContext(
                String globalAgentId,
                TopicConsumer consumer,
                TopicProducer producer,
                TopicAdmin topicAdmin,
                BadRecordHandler brh,
                TopicConnectionProvider topicConnectionProvider,
                Path codeDirectory,
                Path basePersistentStateDirectory,
                Set<String> agentsWithPersistentState,
                MetricsReporter metricsReporter) {
            this.consumer = consumer;
            this.producer = producer;
            this.topicAdmin = topicAdmin;
            this.globalAgentId = globalAgentId;
            this.brh = brh;
            this.topicConnectionProvider = topicConnectionProvider;
            this.codeDirectory = codeDirectory;
            this.basePersistentStateDirectory = basePersistentStateDirectory;
            this.agentsWithPersistentState = agentsWithPersistentState;
            this.metricsReporter = metricsReporter;
            ensurePersistentStateDirectoriesExist();
        }

        @Override
        public MetricsReporter getMetricsReporter() {
            return metricsReporter;
        }

        private void ensurePersistentStateDirectoriesExist() {
            if (basePersistentStateDirectory == null && !agentsWithPersistentState.isEmpty()) {
                throw new IllegalStateException(
                        "Persistent state directory is not configured but some agents have persistent state configured: "
                                + agentsWithPersistentState);
            }
            for (String s : agentsWithPersistentState) {
                Path agentPersistentStateDirectory = basePersistentStateDirectory.resolve(s);
                if (!agentPersistentStateDirectory.toFile().exists()) {
                    throw new IllegalStateException(
                            "Persistent state directory for agent %s does not exist: %s"
                                    .formatted(s, agentPersistentStateDirectory));
                }
            }
        }

        @Override
        public String getGlobalAgentId() {
            return globalAgentId;
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

        @Override
        public void criticalFailure(Throwable error) {
            String errorMsg =
                    "Critical failure: %s. Shutting down the runtime..."
                            .formatted(error.getMessage());
            log.error(errorMsg, error);
            System.err.printf(errorMsg);
            Runtime.getRuntime().halt(1);
        }

        @Override
        public Path getCodeDirectory() {
            return codeDirectory;
        }

        @Override
        public Optional<Path> getPersistentStateDirectoryForAgent(String agentId) {
            if (!agentsWithPersistentState.contains(agentId)) {
                return Optional.empty();
            }
            return Optional.of(basePersistentStateDirectory.resolve(agentId));
        }
    }
}
