package com.dastastax.oss.sga.kafka.runner;

import com.datastax.oss.sga.api.runner.code.Header;
import com.datastax.oss.sga.api.runner.code.Record;
import lombok.ToString;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;

import java.util.ArrayList;
import java.util.List;

@ToString
public class KafkaConsumerRecord implements Record {
    private final ConsumerRecord<?, ?> record;
    private final List<Header> headers = new ArrayList<>();
    private final TopicPartition topicPartition;

    public KafkaConsumerRecord(ConsumerRecord<?, ?> record) {
        this.record = record;
        this.topicPartition = new TopicPartition(record.topic(), record.partition());
        for (org.apache.kafka.common.header.Header header : record.headers()) {
            headers.add(new KafkaHeader(header));
        }
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    @Override
    public Object key() {
        return record.key();
    }

    @Override
    public Object value() {
        return record.value();
    }

    public int estimateRecordSize() {
        return record.serializedKeySize() + record.serializedValueSize();
    }

    public org.apache.kafka.connect.data.Schema keySchema() {
        return null;
    }

    public org.apache.kafka.connect.data.Schema valueSchema() {
        return null;
    }

    @Override
    public String origin() {
        return record.topic();
    }

    public int partition() {
        return record.partition();
    }

    public long offset() {
        return record.offset();
    }

    @Override
    public Long timestamp() {
        return record.timestamp();
    }

    public TimestampType timestampType() {
        return record.timestampType();
    }

    @Override
    public List<Header> headers() {
        return headers;
    }

}
