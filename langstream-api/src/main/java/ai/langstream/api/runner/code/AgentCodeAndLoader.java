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
package ai.langstream.api.runner.code;

import ai.langstream.api.runtime.ComponentType;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

public record AgentCodeAndLoader(AgentCode agentCode, ClassLoader classLoader) {

    public boolean isSource() {
        return agentCode instanceof AgentSource;
    }

    public boolean isSink() {
        return agentCode instanceof AgentSink;
    }

    public boolean isProcessor() {
        return agentCode instanceof AgentProcessor;
    }

    public boolean isService() {
        return agentCode instanceof AgentService;
    }

    public boolean is(Predicate<AgentCode> predicate) {
        return predicate.test(agentCode);
    }

    public void executeWithContextClassloader(RunnableWithException code) throws Exception {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
            code.run(agentCode);
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    public void executeNoExceptionWithContextClassloader(RunnableNoException code) {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
            code.run(agentCode);
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    public <T> T callWithContextClassloader(CallableWithException code) throws Exception {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
            return (T) code.call(agentCode);
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    public <T> T callNoExceptionWithContextClassloader(CallableNoException code) {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
            return (T) code.call(agentCode);
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    public AgentSource asSource() {

        return new AgentSource() {
            @Override
            public List<Record> read() throws Exception {
                return callWithContextClassloader(agentCode -> ((AgentSource) agentCode).read());
            }

            @Override
            public void commit(List<Record> records) throws Exception {
                executeWithContextClassloader(
                        agentCode -> ((AgentSource) agentCode).commit(records));
            }

            @Override
            public String agentId() {
                return callNoExceptionWithContextClassloader(AgentCode::agentId);
            }

            @Override
            public String agentType() {
                return callNoExceptionWithContextClassloader(AgentCode::agentType);
            }

            @Override
            public List<AgentStatusResponse> getAgentStatus() {
                return callNoExceptionWithContextClassloader(AgentCode::getAgentStatus);
            }

            @Override
            public void setMetadata(String id, String agentType, long startedAt) throws Exception {
                executeWithContextClassloader(
                        agentCode -> agentCode.setMetadata(id, agentType, startedAt));
            }

            @Override
            public void init(Map<String, Object> configuration) throws Exception {
                executeWithContextClassloader(agentCode -> agentCode.init(configuration));
            }

            @Override
            public void setContext(AgentContext context) throws Exception {
                executeWithContextClassloader(agentCode -> agentCode.setContext(context));
            }

            @Override
            public void start() throws Exception {
                executeWithContextClassloader(AgentCode::start);
            }

            @Override
            public void close() throws Exception {
                executeWithContextClassloader(AgentCode::close);
            }

            @Override
            public void restart() throws Exception {
                executeWithContextClassloader(AgentCode::restart);
            }

            @Override
            public ComponentType componentType() {
                return callNoExceptionWithContextClassloader(AgentCode::componentType);
            }
        };
    }

    public AgentSink asSink() {

        return new AgentSink() {

            @Override
            public CompletableFuture<?> write(Record record) {
                return callNoExceptionWithContextClassloader(
                        agentCode -> ((AgentSink) agentCode).write(record));
            }

            @Override
            public String agentId() {
                return callNoExceptionWithContextClassloader(AgentCode::agentId);
            }

            @Override
            public String agentType() {
                return callNoExceptionWithContextClassloader(AgentCode::agentType);
            }

            @Override
            public List<AgentStatusResponse> getAgentStatus() {
                return callNoExceptionWithContextClassloader(AgentCode::getAgentStatus);
            }

            @Override
            public void setMetadata(String id, String agentType, long startedAt) throws Exception {
                executeWithContextClassloader(
                        agentCode -> agentCode.setMetadata(id, agentType, startedAt));
            }

            @Override
            public void init(Map<String, Object> configuration) throws Exception {
                executeWithContextClassloader(agentCode -> agentCode.init(configuration));
            }

            @Override
            public void setContext(AgentContext context) throws Exception {
                executeWithContextClassloader(agentCode -> agentCode.setContext(context));
            }

            @Override
            public void start() throws Exception {
                executeWithContextClassloader(AgentCode::start);
            }

            @Override
            public void close() throws Exception {
                executeWithContextClassloader(AgentCode::close);
            }

            @Override
            public void restart() throws Exception {
                executeWithContextClassloader(AgentCode::restart);
            }

            @Override
            public ComponentType componentType() {
                return callNoExceptionWithContextClassloader(AgentCode::componentType);
            }
        };
    }

    public AgentProcessor asProcessor() {

        return new AgentProcessor() {
            @Override
            public void process(List<Record> records, RecordSink recordSink) {
                executeNoExceptionWithContextClassloader(
                        (agentCode -> ((AgentProcessor) agentCode).process(records, recordSink)));
            }

            @Override
            public String agentId() {
                return callNoExceptionWithContextClassloader(AgentCode::agentId);
            }

            @Override
            public String agentType() {
                return callNoExceptionWithContextClassloader(AgentCode::agentType);
            }

            @Override
            public List<AgentStatusResponse> getAgentStatus() {
                return callNoExceptionWithContextClassloader(AgentCode::getAgentStatus);
            }

            @Override
            public void setMetadata(String id, String agentType, long startedAt) throws Exception {
                executeWithContextClassloader(
                        agentCode -> agentCode.setMetadata(id, agentType, startedAt));
            }

            @Override
            public void init(Map<String, Object> configuration) throws Exception {
                executeWithContextClassloader(agentCode -> agentCode.init(configuration));
            }

            @Override
            public void setContext(AgentContext context) throws Exception {
                executeWithContextClassloader(agentCode -> agentCode.setContext(context));
            }

            @Override
            public void start() throws Exception {
                executeWithContextClassloader(AgentCode::start);
            }

            @Override
            public void close() throws Exception {
                executeWithContextClassloader(AgentCode::close);
            }

            @Override
            public void restart() throws Exception {
                executeWithContextClassloader(AgentCode::restart);
            }

            @Override
            public ComponentType componentType() {
                return callNoExceptionWithContextClassloader(AgentCode::componentType);
            }
        };
    }

    public AgentService asService() {

        return new AgentService() {

            @Override
            public String agentId() {
                return callNoExceptionWithContextClassloader(AgentCode::agentId);
            }

            @Override
            public String agentType() {
                return callNoExceptionWithContextClassloader(AgentCode::agentType);
            }

            @Override
            public List<AgentStatusResponse> getAgentStatus() {
                return callNoExceptionWithContextClassloader(AgentCode::getAgentStatus);
            }

            @Override
            public void setMetadata(String id, String agentType, long startedAt) throws Exception {
                executeWithContextClassloader(
                        agentCode -> agentCode.setMetadata(id, agentType, startedAt));
            }

            @Override
            public void init(Map<String, Object> configuration) throws Exception {
                executeWithContextClassloader(agentCode -> agentCode.init(configuration));
            }

            @Override
            public void setContext(AgentContext context) throws Exception {
                executeWithContextClassloader(agentCode -> agentCode.setContext(context));
            }

            @Override
            public void start() throws Exception {
                executeWithContextClassloader(AgentCode::start);
            }

            @Override
            public void join() throws Exception {
                executeWithContextClassloader(agentCode -> ((AgentService) agentCode).join());
            }

            @Override
            public void close() throws Exception {
                executeWithContextClassloader(AgentCode::close);
            }

            @Override
            public void restart() throws Exception {
                executeWithContextClassloader(AgentCode::restart);
            }

            @Override
            public ComponentType componentType() {
                return callNoExceptionWithContextClassloader(AgentCode::componentType);
            }
        };
    }

    public void close() throws Exception {
        executeWithContextClassloader(AgentCode::close);
    }

    @FunctionalInterface
    public interface RunnableWithException {
        void run(AgentCode agent) throws Exception;
    }

    @FunctionalInterface
    public interface RunnableNoException {
        void run(AgentCode agent);
    }

    @FunctionalInterface
    public interface CallableWithException {
        Object call(AgentCode agent) throws Exception;
    }

    @FunctionalInterface
    public interface CallableNoException {
        Object call(AgentCode agent);
    }
}
