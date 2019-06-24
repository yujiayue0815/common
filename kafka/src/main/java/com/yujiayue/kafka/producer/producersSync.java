package com.yujiayue.kafka.producer;

import com.yujiayue.util.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author : 余嘉悦
 * @date : 2019/4/27 11:38
 * @description : 同步生产者
 * 每次生产一条数据 确保收到返回消息后再继续生产,同步时生产消息的效率较底，但是能够保证数据的可靠性
 */
public class producersSync {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = Resources.get("kafka-producer.properties");
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 100; i++) {
            Future<RecordMetadata> send = producer.send(new ProducerRecord<>(TopicEnum.FIRST.getValue(), String.valueOf(i), "MESSAGE-" + i));
            RecordMetadata metadata = send.get();
            System.out.print("topic : " + metadata.topic());
            System.out.print("\tpartition : " + metadata.partition());
            System.out.print("\toffset : " + metadata.offset());
            System.out.println("\ttimestamp : " + metadata.timestamp());
        }
        producer.close();
    }
}
