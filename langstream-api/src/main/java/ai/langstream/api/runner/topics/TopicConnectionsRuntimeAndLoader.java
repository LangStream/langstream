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
package ai.langstream.api.runner.topics;

import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runtime.ExecutionPlan;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public record TopicConnectionsRuntimeAndLoader(
        TopicConnectionsRuntime connectionsRuntime, ClassLoader classLoader) {

    public void executeWithContextClassloader(RunnableWithException code) throws Exception {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
            code.run(connectionsRuntime);
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    public void executeNoExceptionWithContextClassloader(RunnableNoException code) {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
            code.run(connectionsRuntime);
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    public <T> T callWithContextClassloader(CallableWithException code) throws Exception {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
            return (T) code.call(connectionsRuntime);
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    public <T> T callNoExceptionWithContextClassloader(CallableNoException code) {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
            return (T) code.call(connectionsRuntime);
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    public TopicConnectionsRuntime asTopicConnectionsRuntime() {

        return new TopicConnectionsRuntime() {

            @Override
            public void init(StreamingCluster streamingCluster) {
                executeNoExceptionWithContextClassloader(code -> code.init(streamingCluster));
            }

            @Override
            public void deploy(ExecutionPlan applicationInstance) {
                executeNoExceptionWithContextClassloader(code -> code.deploy(applicationInstance));
            }

            @Override
            public void delete(ExecutionPlan applicationInstance) {
                executeNoExceptionWithContextClassloader(code -> code.delete(applicationInstance));
            }

            @Override
            public void close() {
                executeNoExceptionWithContextClassloader(TopicConnectionsRuntime::close);
            }

            @Override
            public TopicConsumer createConsumer(
                    String agentId,
                    StreamingCluster streamingCluster,
                    Map<String, Object> configuration) {

                final TopicConsumer topicConsumerImpl =
                        (TopicConsumer)
                                callNoExceptionWithContextClassloader(
                                        code ->
                                                code.createConsumer(
                                                        agentId, streamingCluster, configuration));
                return new TopicConsumer() {
                    @Override
                    public long getTotalOut() {
                        return callNoExceptionWithContextClassloader(
                                ignore -> topicConsumerImpl.getTotalOut());
                    }

                    @Override
                    public Object getNativeConsumer() {
                        return callNoExceptionWithContextClassloader(
                                ignore -> topicConsumerImpl.getNativeConsumer());
                    }

                    @Override
                    public void start() throws Exception {
                        executeWithContextClassloader(ignore -> topicConsumerImpl.start());
                    }

                    @Override
                    public void close() throws Exception {
                        executeWithContextClassloader(ignore -> topicConsumerImpl.close());
                    }

                    @Override
                    public List<Record> read() throws Exception {
                        return callWithContextClassloader(ignore -> topicConsumerImpl.read());
                    }

                    @Override
                    public void commit(List<Record> records) throws Exception {
                        executeWithContextClassloader(ignore -> topicConsumerImpl.commit(records));
                    }

                    @Override
                    public Map<String, Object> getInfo() {
                        return callNoExceptionWithContextClassloader(
                                ignore -> topicConsumerImpl.getInfo());
                    }
                };
            }

            @Override
            public TopicReader createReader(
                    StreamingCluster streamingCluster,
                    Map<String, Object> configuration,
                    TopicOffsetPosition initialPosition) {
                TopicReader topicReaderImpl =
                        callNoExceptionWithContextClassloader(
                                code ->
                                        code.createReader(
                                                streamingCluster, configuration, initialPosition));
                if (topicReaderImpl == null) {
                    return null;
                }

                return new TopicReader() {
                    @Override
                    public void start() throws Exception {
                        executeWithContextClassloader(ignore -> topicReaderImpl.start());
                    }

                    @Override
                    public void close() throws Exception {
                        executeWithContextClassloader(ignore -> topicReaderImpl.close());
                    }

                    @Override
                    public TopicReadResult read() throws Exception {
                        return callWithContextClassloader(ignore -> topicReaderImpl.read());
                    }
                };
            }

            @Override
            public TopicProducer createProducer(
                    String agentId,
                    StreamingCluster streamingCluster,
                    Map<String, Object> configuration) {
                final TopicProducer topicProducerImpl =
                        callNoExceptionWithContextClassloader(
                                code ->
                                        code.createProducer(
                                                agentId, streamingCluster, configuration));
                if (topicProducerImpl == null) {
                    return null;
                }

                return wrapTopicProducer(topicProducerImpl);
            }

            private TopicProducer wrapTopicProducer(TopicProducer topicProducerImpl) {
                return new TopicProducer() {
                    @Override
                    public long getTotalIn() {
                        return callNoExceptionWithContextClassloader(
                                ignore -> topicProducerImpl.getTotalIn());
                    }

                    @Override
                    public void start() {
                        executeNoExceptionWithContextClassloader(
                                ignore -> topicProducerImpl.start());
                    }

                    @Override
                    public void close() {
                        executeNoExceptionWithContextClassloader(
                                ignore -> topicProducerImpl.close());
                    }

                    @Override
                    public CompletableFuture<?> write(Record record) {
                        return callNoExceptionWithContextClassloader(
                                ignore -> topicProducerImpl.write(record));
                    }

                    @Override
                    public Object getNativeProducer() {
                        return callNoExceptionWithContextClassloader(
                                ignore -> topicProducerImpl.getNativeProducer());
                    }

                    @Override
                    public Object getInfo() {
                        return callNoExceptionWithContextClassloader(
                                ignore -> topicProducerImpl.getInfo());
                    }
                };
            }

            @Override
            public TopicProducer createDeadletterTopicProducer(
                    String agentId,
                    StreamingCluster streamingCluster,
                    Map<String, Object> configuration) {
                final TopicProducer producerImpl =
                        callNoExceptionWithContextClassloader(
                                code ->
                                        code.createDeadletterTopicProducer(
                                                agentId, streamingCluster, configuration));
                if (producerImpl == null) {
                    return null;
                }
                return wrapTopicProducer(producerImpl);
            }

            @Override
            public TopicAdmin createTopicAdmin(
                    String agentId,
                    StreamingCluster streamingCluster,
                    Map<String, Object> configuration) {
                final TopicAdmin topicAdminImpl =
                        callNoExceptionWithContextClassloader(
                                code ->
                                        code.createTopicAdmin(
                                                agentId, streamingCluster, configuration));
                if (topicAdminImpl == null) {
                    return null;
                }
                return new TopicAdmin() {
                    @Override
                    public void start() {
                        executeNoExceptionWithContextClassloader(ignore -> topicAdminImpl.start());
                    }

                    @Override
                    public void close() {
                        executeNoExceptionWithContextClassloader(ignore -> topicAdminImpl.close());
                    }

                    @Override
                    public Object getNativeTopicAdmin() {
                        return callNoExceptionWithContextClassloader(
                                ignore -> topicAdminImpl.getNativeTopicAdmin());
                    }
                };
            }
        };
    }

    public void close() throws Exception {
        executeWithContextClassloader(TopicConnectionsRuntime::close);
    }

    @FunctionalInterface
    public interface RunnableWithException {
        void run(TopicConnectionsRuntime agent) throws Exception;
    }

    @FunctionalInterface
    public interface RunnableNoException {
        void run(TopicConnectionsRuntime agent);
    }

    @FunctionalInterface
    public interface CallableWithException {
        Object call(TopicConnectionsRuntime agent) throws Exception;
    }

    @FunctionalInterface
    public interface CallableNoException {
        Object call(TopicConnectionsRuntime agent);
    }
}
