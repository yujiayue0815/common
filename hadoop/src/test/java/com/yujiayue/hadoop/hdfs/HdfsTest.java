package com.yujiayue.hadoop.hdfs;

import com.yujiayue.hadoop.hdfs.tool.HdfsTool;
import org.apache.hadoop.fs.FileStatus;
import org.junit.Test;

import java.io.IOException;

public class HdfsTest {

    @Test
    public void listStatus() {

        try {
            FileStatus[] fileStatuses = HdfsTool.listStatus("/");
            for (FileStatus fileStatus : fileStatuses) {
                System.out.println(fileStatus.getPath().toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("ok");
    }
}
