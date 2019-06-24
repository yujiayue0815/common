package com.yujiayue.hbase;

import com.yujiayue.util.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author : 余嘉悦
 * @date : 2019/4/29 9:58
 * @description : Hbase database client utils.
 */
public class Hbases {
    private static final Logger log = LoggerFactory.getLogger(Hbases.class);

    public static final String ZOOKEEPER = "hadoop100,hadoop101,hadoop102";

    private static Connection connection = null;

    private static ThreadLocal<Admin> localAdmin = new ThreadLocal<>();


    /**
     * 获取hbase 连接，该连接是一个线程安全的，所有的线程公用一个connection
     *
     * @return
     * @throws IOException
     */
    private synchronized static Connection getConnection() throws IOException {
        if (connection == null) {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", ZOOKEEPER);
            connection = ConnectionFactory.createConnection(configuration);
        }
        return connection;
    }

    /**
     * 删除命名空间
     *
     * @param nameSpace
     * @throws IOException
     */
    public static void deleteNameSpace(String nameSpace) throws IOException {
        Admin admin = getAdmin();
        admin.deleteNamespace(nameSpace);
    }

    /**
     * 创建需要的表
     *
     * @param nameSpace
     * @param table
     * @param columnFamily
     */
    public static void createTable(String nameSpace, String table, String... columnFamily) throws IOException {
        createTable(nameSpace + ":" + table, columnFamily);
    }

    /**
     * 获取Admin admin不是一个线程安全的
     *
     * @return
     * @throws IOException
     */
    public static Admin getAdmin() throws IOException {
        Admin admin = localAdmin.get();
        if (admin == null) {
            admin = getConnection().getAdmin();
            localAdmin.set(admin);
        }
        return admin;
    }

    /**
     * 创建命名空间
     *
     * @param nameSpace
     */
    public static void createNamespace(String nameSpace, Map<String, String> configuration) throws IOException {
        Admin admin = getAdmin();
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).addConfiguration(configuration).build();
        admin.createNamespace(namespaceDescriptor);

    }

    /**
     * 创建表 必须指定列族
     *
     * @param tableName
     * @param columnFamily
     * @throws IOException
     */
    public static void createTable(String tableName, String... columnFamily) throws IOException {
        if (!exists(tableName)) {
            Admin admin = getAdmin();
            TableName name = TableName.valueOf(tableName);
            HTableDescriptor descriptor = new HTableDescriptor(name);
            for (String fam : columnFamily) {
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(fam);
                descriptor.addFamily(columnDescriptor);
            }
            admin.createTable(descriptor);
            log.info("table " + tableName + "is create success");
        } else {
            log.warn("table " + tableName + "is exists");
        }
    }

    /**
     * 创建表 必须指定列族
     *
     * @param tableName
     * @param columnFamily
     * @throws IOException
     */
    public static void createTable(String nameSpace, String tableName, FamilyParam... columnFamily) throws IOException {
        createTable(nameSpace + ":" + tableName, columnFamily);
    }


    /**
     * 创建表 必须指定列族
     *
     * @param tableName
     * @param columnFamily
     * @throws IOException
     */
    public static void createTable(String tableName, FamilyParam... columnFamily) throws IOException {
        if (!exists(tableName)) {
            Admin admin = getAdmin();
            TableName name = TableName.valueOf(tableName);
            HTableDescriptor descriptor = new HTableDescriptor(name);
            for (FamilyParam fam : columnFamily) {
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(fam.getFamily());
                descriptor.addFamily(fam.autoConfig(columnDescriptor));
            }
            admin.createTable(descriptor);
            log.info("table " + tableName + "is create success");
        } else {
            log.warn("table " + tableName + "is exists");
        }
    }

    /**
     * 判断是否存在
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean exists(String tableName) throws IOException {
        Admin admin = getAdmin();
        return admin.tableExists(TableName.valueOf(tableName));
    }


    public static void deleteTable(String nameSpace, String tableName) throws IOException {
        deleteTable(nameSpace + ":" + tableName);
    }

    /**
     * 删除表
     *
     * @param tableName
     * @throws IOException
     */
    public static void deleteTable(String tableName) throws IOException {
        if (exists(tableName)) {
            Admin admin = getAdmin();
            TableName name = TableName.valueOf(tableName);
            admin.disableTable(name);
            admin.deleteTable(name);
            log.warn("table " + tableName + " delete success");
        } else {
            log.warn("table " + tableName + "is not exists");
        }
    }


    /**
     * Delete table's more row
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void delete(String tableName, String... rowKey) throws IOException {
        Table table = getTable(tableName);
        List<Delete> deletes = new ArrayList<>();
        for (String key : rowKey) {
            deletes.add(new Delete(Bytes.toBytes(key)));
        }
        table.delete(deletes);
        table.close();
    }

    /**
     * Delete table's column family's column
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @throws IOException
     */
    public static void delete(String tableName, String rowKey, String columnFamily, String column) throws IOException {
        Table table = getTable(tableName);
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
        table.delete(delete);
        table.close();
    }

    /**
     * Get an row data
     *
     * @param tableName
     * @param rowKey
     * @return
     * @throws IOException
     */
    public static List<Map<String, String>> get(String tableName, String rowKey) throws IOException {
        List<Map<String, String>> res = new ArrayList<>();
        Table table = getTable(tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            Map<String, String> item = rowMap(cell);
            res.add(item);
        }
        return res;
    }

    /**
     * get data
     *
     * @param nameSpace
     * @param tableName
     * @param rowKey
     * @return
     * @throws IOException
     */
    public static List<Map<String, String>> get(String nameSpace, String tableName, String rowKey) throws IOException {
        return get(nameSpace + ":" + tableName, rowKey);
    }

    /**
     * 扫描表
     *
     * @param tableName
     * @param startRow
     * @param endRow
     * @return
     * @throws IOException
     */
    public static List<Map<String, String>> get(String tableName, Long startRow, Long endRow) throws IOException {
        return get(tableName, null, startRow, endRow);
    }

    /**
     * scan more row data
     *
     * @param tableName
     * @param startRow
     * @param endRow
     * @return
     * @throws IOException
     */
    public static List<Map<String, String>> get(String tableName, String family, long startRow, long endRow) throws IOException {
        List<Map<String, String>> res = new ArrayList<>();
        Table table = getTable(tableName);
        Scan scan = null;
        if (startRow != 0 && endRow != 0) {
            scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(endRow));
        } else {
            scan = new Scan();
        }

        if (Strings.notEmpty(family)) {
            scan.addFamily(Bytes.toBytes(family));
        }
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                Map<String, String> item = rowMap(cell);
                res.add(item);
            }
        }

        return res;
    }

    /**
     * 单元格
     *
     * @param cell
     * @return
     */
    private static Map<String, String> rowMap(Cell cell) {
        Map<String, String> item = new HashMap<>();
        item.put("rowKey", Bytes.toString(CellUtil.cloneRow(cell)));
        item.put("family", Bytes.toString(CellUtil.cloneFamily(cell)));
        item.put("qualifier", Bytes.toString(CellUtil.cloneQualifier(cell)));
        item.put("value", Bytes.toString(CellUtil.cloneValue(cell)));
        return item;
    }

    /**
     * insert cell to table
     *
     * @param nameSpace
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param value
     * @throws IOException
     */
    public static void put(String nameSpace, String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {
        put(nameSpace + ":" + tableName, rowKey, columnFamily, column, value);
    }


    /**
     * insert cell to table
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param value
     */
    public static void put(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {
        Table table = getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
        table.close();
    }


    /**
     * Get table's instance
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public static Table getTable(String tableName) throws IOException {
        Table table = null;
        if (exists(tableName)) {
            table = getConnection().getTable(TableName.valueOf(tableName));
        }
        return table;
    }

    public static void close() throws IOException {
        if (connection != null)
            connection.close();
    }
}
