package com.yujiayue.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author : 余嘉悦
 * @date : 2019/4/27 18:04
 * @description :  producer 生产消息，生产后的消息会使用producer.send方法 发送收到kafka上
 * 在此之前会先将数据经过 拦截器链 之后经行序列化器，然后经行分区器
 */
public class TimeInterceptor implements ProducerInterceptor<String, String> {


    /**
     * 每条消息都会经过拦截器，如果拦截器返回null 该条消息将被拦截掉，
     * 在拦截器中对消息经行修改的时候需要重新创建ProducerRecord
     *
     * @param record
     * @return
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), record.key(), System.currentTimeMillis() + record.value(), record.headers());
    }

    /**
     * 消息经过拦截器，序列化，和分区后主线程会将消息放入 RecordAccumulator 中sender 线程会将消息从RecordAccumulator中拉取到然后通过网络发送到kafka中
     * 该方法会在主线程发送的过程后或者发送失败时调用
     * @param metadata
     * @param exception
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
