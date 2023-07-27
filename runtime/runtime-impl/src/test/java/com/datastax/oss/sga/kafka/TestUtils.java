package com.datastax.oss.sga.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.awaitility.Awaitility;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class TestUtils {
    public static void waitForMessages(KafkaConsumer consumer,
                                       List<String> expected) throws Exception {
        List<String> received = new ArrayList<>();

        Awaitility.await().untilAsserted(() -> {
                    ConsumerRecords<String, String> poll = consumer.poll(Duration.ofSeconds(2));
                    for (ConsumerRecord record : poll) {
                        log.info("Received message {}", record);
                        received.add(record.value().toString());
                    }
                    log.info("Result: {}", received);
                    received.forEach(r -> {
                        log.info("Received |{}|", r);
                    });
                    assertEquals(expected, received);
                }
        );
    }
}
