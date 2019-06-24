package com.yujiayue.kafka.consumer;

import com.yujiayue.kafka.producer.TopicEnum;
import com.yujiayue.util.Resources;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author : 余嘉悦
 * @date : 2019/4/27 11:56
 * @description : kafka consumer 手动提交offset ，该commitSync
 *
 */
public class ConsumerSyncOffset {
    public static void main(String[] args) {
        Properties properties = Resources.get("kafka-consumer.properties");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(TopicEnum.FIRST.getValue()));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.value());
                }
                //同步的提交方式
                consumer.commitSync();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
