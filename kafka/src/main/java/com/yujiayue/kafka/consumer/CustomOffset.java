package com.yujiayue.kafka.consumer;

import com.yujiayue.kafka.producer.TopicEnum;
import com.yujiayue.util.Resources;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.*;


//分区分配的重新设计，每个消费者组中的消费者都会对应一个topic中的分区，当消费者组发生改变的时候消费者和分区之间的对应关系就会发生改变，此时需要重新的进行
//进行分区分配
//消费者在拿到新的分区之后要先获取到每个分区中最新的消费的offset，并继续消费
public class CustomOffset {

    private static Map<TopicPartition, Long> offsetMap = new HashMap<>();

    public static void main(String[] args) {
        Properties properties = Resources.get("kafka-consumer-auto.properties");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(TopicEnum.FIRST.getValue()), new ConsumerRebalanceListener() {

            //该方法会在重新进行分区的对应前进行调用
            //重新进行分区对应的方法是reBalance
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                commitOffsets(offsetMap);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                offsetMap.clear();
                for (TopicPartition partition : partitions) {
                    //每个分区获取最新的offset的位置
                    long offset = getOffset(partition.topic(), partition.partition());
                    offsetMap.put(new TopicPartition(partition.topic(), partition.partition()), offset);
                }
            }

            /**
             * 获取topic下partition
             * @param topic
             * @param partition
             * @return
             */
            private long getOffset(String topic, int partition) {
                return 0;
            }

        });


        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.value());
                    //拉取到消息后需要将消息的offset保存
                    offsetMap.put(new TopicPartition(record.topic(), record.partition()), record.offset());
                }
                //此处需要将消费的消息的offset进行commit
                commitOffsets(offsetMap);

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    /**
     * 将消息进行提交
     * @param offsetMap
     */
    private static void commitOffsets(Map<TopicPartition, Long> offsetMap) {

    }
}
