package com.datastax.oss.sga.runtime.agent.api;

import com.datastax.oss.sga.api.runner.code.AgentProcessor;
import com.datastax.oss.sga.api.runner.code.AgentSink;
import com.datastax.oss.sga.api.runner.code.AgentSource;
import com.datastax.oss.sga.api.runner.topics.TopicConsumer;
import com.datastax.oss.sga.api.runner.topics.TopicProducer;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class AgentInfo {
    private TopicConsumer consumer;
    private TopicProducer producer;
    private AgentProcessor processor;
    private AgentSource source;
    private AgentSink sink;

    public void watchConsumer(TopicConsumer consumer) {
        this.consumer = consumer;
    }

    public void watchProducer(TopicProducer producer) {
        this.producer = producer;
    }

    public void watchProcessor(AgentProcessor processor) {
        this.processor = processor;
    }

    public void watchSource(AgentSource source) {
        this.source = source;
    }

    public void watchSink(AgentSink sink) {
        this.sink = sink;
    }


    /**
     * This is serving the data to the Control Plane,
     * changing the format is a breaking change, please take care to backward compatibility.
     * @return
     */
    public Map<String, Object> serveInfos() {
        Map<String, Object> result = new LinkedHashMap<>();
        if (consumer != null) {
            result.put("consumer", consumer.getInfo());
        }
        if (producer != null) {
            result.put("producer", producer.getInfo());
        }
        if (source != null) {
            result.put("source", source.getInfo());
        }
        if (processor != null) {
            result.put("processor", processor.getInfo());
        }
        if (sink != null) {
            result.put("sink", sink.getInfo());
        }
        return result;
    }

}
