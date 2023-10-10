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
package ai.langstream.webservice.application;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import ai.langstream.api.storage.ApplicationStore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;
import reactor.core.publisher.FluxSink;

class ApplicationResourceLogsTest {

    @ParameterizedTest
    @EnumSource(ApplicationResource.ApplicationLogsFormats.class)
    void testLogLine(ApplicationResource.ApplicationLogsFormats format) {

        AtomicLong lastSent = new AtomicLong();

        final ApplicationStore.PodLogHandler podLogHandler =
                new ApplicationStore.PodLogHandler() {
                    @Override
                    public void start(ApplicationStore.LogLineConsumer onLogLine) {}

                    @Override
                    public String getPodName() {
                        return "mypod-0";
                    }

                    @Override
                    public void close() {}
                };
        final FluxSink fluxSink = Mockito.mock(FluxSink.class);
        AtomicReference<String> output = new AtomicReference<>();
        when(fluxSink.next(any()))
                .thenAnswer(
                        invocationOnMock -> {
                            output.set(invocationOnMock.getArguments()[0] + "");
                            return null;
                        });
        final ApplicationResource.LogLineConsumer lineConsumer =
                new ApplicationResource.LogLineConsumer(
                        podLogHandler, value -> lastSent.set(value), fluxSink, format);

        ApplicationStore.LogLineResult result = lineConsumer.onLogLine("line logs", 1696926662058L);
        assertTrue(result.continueLogging());
        assertNull(result.delayInSeconds());

        if (format == ApplicationResource.ApplicationLogsFormats.text) {
            assertEquals("\u001B[32m[mypod-0] line logs\u001B[0m\n", output.get());
        } else {
            assertEquals(
                    "{\"replica\":\"mypod-0\",\"message\":\"line logs\",\"timestamp\":1696926662058}\n",
                    output.get());
        }

        result = lineConsumer.onPodNotRunning("Error", "Some long exception");
        assertTrue(result.continueLogging());
        assertEquals(10L, result.delayInSeconds());
        if (format == ApplicationResource.ApplicationLogsFormats.text) {
            assertEquals(
                    "\u001B[32m[mypod-0] Replica mypod-0 is not running, will retry in 10 seconds. State: Error,"
                            + " Reason: Some long exception\u001B[0m\n",
                    output.get());
        } else {
            assertEquals(
                    "{\"replica\":\"mypod-0\",\"message\":\"Replica mypod-0 is not running, will retry in 10 seconds"
                            + ". State: Error, Reason: Some long exception\"}\n",
                    output.get());
        }

        result = lineConsumer.onPodLogNotAvailable();
        assertTrue(result.continueLogging());
        assertEquals(10L, result.delayInSeconds());
        if (format == ApplicationResource.ApplicationLogsFormats.text) {
            assertEquals(
                    "\u001B[32m[mypod-0] Replica mypod-0 logs not available, will retry in 10 seconds\u001B[0m\n",
                    output.get());
        } else {
            assertEquals(
                    "{\"replica\":\"mypod-0\",\"message\":\"Replica mypod-0 logs not available, will retry in 10 "
                            + "seconds\"}\n",
                    output.get());
        }
    }
}
