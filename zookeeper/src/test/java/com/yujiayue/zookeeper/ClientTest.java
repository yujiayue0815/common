package com.yujiayue.zookeeper;

import org.junit.Test;

public class ClientTest {

    @Test
    public void getClient() {

        for (int i = 0; i < 10; i++) {

             Client.getChildren("/");

            Client.close();
        }
        System.out.println("ok");
    }
}
