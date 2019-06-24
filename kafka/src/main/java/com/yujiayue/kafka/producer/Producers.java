package com.yujiayue.kafka.producer;

import com.yujiayue.util.Resources;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author : 余嘉悦
 * @date : 2019/4/27 10:15
 * @description : kafka producer 简单的发送发送消息的方式
 */
public class Producers {

    private static final Logger log = LoggerFactory.getLogger(Producers.class);
    private static Properties properties = Resources.get("kafka-producer.properties");


    public static void sendMapCallBack(TopicEnum topic, Map<String, String> map) {
        sendMapCallBack(topic, map, null);
    }


    public static void sendList(TopicEnum topic, List<String> list) {
        sendListCallBack(topic, list, null);
    }

    public static void sendListCallBack(TopicEnum topic, List<String> list, Callback callback) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        try {
            list.forEach(v -> {
                if (callback == null)
                    producer.send(new ProducerRecord<String, String>(topic.getValue(), v));
                else
                    producer.send(new ProducerRecord<String, String>(topic.getValue(), v), callback);
            });
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    public static void sendMapCallBack(TopicEnum topic, Map<String, String> map, Callback callback) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        try {
            map.forEach((k, v) -> {
                if (callback == null)
                    producer.send(new ProducerRecord<String, String>(topic.getValue(), k, v));
                else
                    producer.send(new ProducerRecord<String, String>(topic.getValue(), k, v), callback);
            });
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    public static void sendCallBack(TopicEnum topic, String value) {
        simpleSend(topic, null, value, null);
    }

    public static void simpleSend(TopicEnum topic, String value) {
        simpleSend(topic, null, value, null);
    }

    /**
     * Simple producer send
     *
     * @param topic kafka 主题
     * @param key   message key
     * @param value message
     */
    public static void simpleSend(TopicEnum topic, String key, String value, Callback callback) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        try {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic.getValue(), key, value);
            if (callback == null)
                producer.send(record);
            else
                producer.send(record, callback);
        } finally {
            producer.close();
        }
        log.info("send success");
    }


}
