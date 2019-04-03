package com.yujiayue.hadoop.hdfs;

import com.yujiayue.hadoop.hdfs.tool.HdfsTool;
import org.junit.Test;

public class HdfsTest {

    @Test
    public void mkdir(){

        HdfsTool.mkdir("/api/test");
        System.out.println("ok");
    }
}
