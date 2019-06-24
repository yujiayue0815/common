package com.yujiayue.hbase;

import java.io.IOException;

public class ClientTest {
    public static void main(String[] args) {

        try {
            Hbases.deleteTable("hello");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                Hbases.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
