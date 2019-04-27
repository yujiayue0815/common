package com.yujiayue.kafka.producer;

import com.yujiayue.util.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author : 余嘉悦
 * @date : 2019/4/27 10:15
 * @description : kafka producer 简单的发送发送消息的方式
 */
public class Producers {

    private static final Logger log = LoggerFactory.getLogger(Producers.class);
    private static Properties properties = Resources.get("kafka-producer.properties");


    /**
      * @author : 余嘉悦
      * @date : 2019/4/27 10:38
      * @description : Simple
      *
      */
    public static void simpleSend(TopicEnum topic, String key, String value) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        try {
            if (Strings.isEmpty(key))
                producer.send(new ProducerRecord<>(topic.getValue(), value));
            else
                producer.send(new ProducerRecord<>(topic.getValue(), key, value));
        } finally {
            producer.close();
        }
        log.info("send success");
    }

}
