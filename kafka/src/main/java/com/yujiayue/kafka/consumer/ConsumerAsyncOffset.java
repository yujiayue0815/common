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
 * @description : kafka consumer 手动提交offset，采用异步的提交方式，
 *  consumer 会将offset 提交到kafka的_consumer_offsets 中，同步的方式是当offset 写入到_consumer_offset中并返回成功之后
 *  才会进行下一次的consumer数据的拉取和消费，当消息写入失败的时候会有消息的重试，在异步的时候只是将offset发送出去的时候就不会等到
 *  offset返回成功就会直接开始拉去数据，进行下一次的消费
 *
 */
public class ConsumerAsyncOffset {
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
                //异步的提交方式
                consumer.commitAsync();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
