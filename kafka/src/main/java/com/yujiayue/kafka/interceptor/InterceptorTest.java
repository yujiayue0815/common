package com.yujiayue.kafka.interceptor;

import com.yujiayue.kafka.producer.Producers;
import com.yujiayue.kafka.producer.TopicEnum;

import java.util.ArrayList;
import java.util.List;

public class InterceptorTest {
    public static void main(String[] args) {
        List<String> msg = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            msg.add("MESSAGE-" + i);
        }

        Producers.sendList(TopicEnum.FIRST, msg);
    }
}
