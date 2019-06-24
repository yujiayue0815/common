package com.yujiayue.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class CounterInterceptor implements ProducerInterceptor<String, String> {

    private long totalCounter = 0L;
    private long failCounter = 0L;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        totalCounter++;
        if (exception != null) {
            failCounter++;
        }
    }

    @Override
    public void close() {

        System.out.println("total counter:" + totalCounter);
        System.out.println("fail counter:" + failCounter);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
