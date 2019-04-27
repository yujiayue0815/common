package com.yujiayue.kafka.producer;

import com.yujiayue.util.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducersCallback {


    public static void main(String[] args) {


        Properties properties = Resources.get("kafka-producer.properties");
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        for (int i = 0; i < 1000; i++) {
            producer.send(new ProducerRecord<>(TopicEnum.FIRST.getValue(), String.valueOf(i), "MESSAGE-" + i), (metadata, exception) -> {
                if (exception == null) {
                    System.out.print("topic : " + metadata.topic());
                    System.out.print("\tpartition : " + metadata.partition());
                    System.out.print("\toffset : " + metadata.offset());
                    System.out.println("\ttimestamp : " + metadata.timestamp());
                }
            });
        }
        producer.close();
    }
}
