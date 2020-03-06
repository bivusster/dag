package com.arb.clickhouse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

public class HBaseClient {
    private Configuration config;
    private Connection connection;
    private Logger log = Logger.getLogger(HBaseClient.class.getName());
    private static final String TABLE_NAME = "event";
    private static final String COLUMN_FAMILY = "event_info";
    private static final String ROW_KEY = "d6eb8287-afec-4854-bea1-cba988cb1d85";

    public HBaseClient() {
        config = HBaseConfiguration.create();
        config.clear();
        config.set("hbase.zookeeper.quorum", "192.168.1.225");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.master.port", "16000");
        config.set("hbase.master.info.port", "16010");
        config.set("hbase.regionserver.port", "16020");
        config.set("hbase.regionserver.info.port", "16010");
        config.set("hbase.localcluster.port.ephemeral", "false");
    }

    public void createTable() {
        try {
            connection = ConnectionFactory.createConnection(config);
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(TABLE_NAME);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            if(!admin.tableExists(tableName)) {
                log.info("Table creation...");
                TableDescriptor htable = TableDescriptorBuilder.newBuilder(tableName)
                        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(COLUMN_FAMILY))
                        .build();
                admin.createTable(htable);
                log.info("Table was created");
            } else {
                log.warning("Table already exists");
            }
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void insertOneRow() {
        try {
            connection = ConnectionFactory.createConnection(config);
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(TABLE_NAME);
            if(admin.tableExists(tableName)) {
                Table table = connection.getTable(tableName);

                int userId = (int) Math.ceil(100 * Math.random());
                int itemId = (int) Math.ceil(1000 * Math.random());
                Put put = new Put(Bytes.toBytes(ROW_KEY));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("event"), Bytes.toBytes("view"));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("entityType"), Bytes.toBytes("user"));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("entityId"), Bytes.toBytes(userId));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("targetEntityType"), Bytes.toBytes("item"));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("targetEntityId"), Bytes.toBytes(itemId));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("eventTime"), Bytes.toBytes(System.currentTimeMillis()));

                long start = System.currentTimeMillis();
                table.put(put);
                long end = System.currentTimeMillis();

                table.close();
                connection.close();

                log.info("Row has been insert. Time - " + (end - start) + " ms.");
            } else {
                log.warning("Table does not exist");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void readOneRow(boolean displayReadData) {
        try {
            connection = ConnectionFactory.createConnection(config);
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(TABLE_NAME);
            if(admin.tableExists(tableName)) {
                Table table = connection.getTable(tableName);
                Get get = new Get(Bytes.toBytes(ROW_KEY));

                long start = System.currentTimeMillis();
                Result set = table.get(get);
                long end = System.currentTimeMillis();

                table.close();
                connection.close();

                log.info("Row has been read. Time - " + (end - start) + " ms.");

                if(displayReadData) {
                    Cell[] cells  = set.rawCells();
                    for(Cell cell : cells) {
                        System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())+"::"+
                                Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    }
                }

            } else {
                log.warning("Table does not exist");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void insertManyRows(int count) {
        count = 100000;
        try {
            connection = ConnectionFactory.createConnection(config);
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(TABLE_NAME);
            if(admin.tableExists(tableName)) {
                Table table = connection.getTable(tableName);
                List<Put> puts = new ArrayList<Put>();
                Object[] result = new Object[count];

                for(int i = 0; i < count; i++) {
                    String rowKey = UUID.randomUUID().toString();
                    int userId = (int) Math.ceil(100 * Math.random());
                    int itemId = (int) Math.ceil(1000 * Math.random());
                    Put put = new Put(Bytes.toBytes(rowKey));
                    put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("event"), Bytes.toBytes("view"));
                    put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("entityType"), Bytes.toBytes("user"));
                    put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("entityId"), Bytes.toBytes(userId));
                    put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("targetEntityType"), Bytes.toBytes("item"));
                    put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("targetEntityId"), Bytes.toBytes(itemId));
                    put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("eventTime"), Bytes.toBytes(System.currentTimeMillis()));
                    puts.add(put);
                }

                long start = System.currentTimeMillis();
                table.batch(puts, result);
                long end = System.currentTimeMillis();

                table.close();
                connection.close();

                log.info("Rows has been insert. Count - " + count + ". Time - " + (end - start) + " ms.");
            } else {
                String rowKey = UUID.randomUUID().toString();
                log.warning("Table does not exist");
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void readManyRows() {
        try {
            connection = ConnectionFactory.createConnection(config);
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(TABLE_NAME);
            if(admin.tableExists(tableName)) {
                Table table = connection.getTable(tableName);
                Scan scanner = new Scan();

                long start = System.currentTimeMillis();
                ResultScanner rscan = table.getScanner(scanner);
                long end = System.currentTimeMillis();

                long count = 0;
                for(Result rs: rscan) {
                    count++;
                }

                table.close();
                connection.close();

                log.info("Rows has been read. Count - " + count + ". Time - " + (end - start) + " ms.");
            } else {
                String rowKey = UUID.randomUUID().toString();
                log.warning("Table does not exist");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void runAllTest() {
        createTable();
        insertOneRow();
        readOneRow(false);
        insertManyRows(60000);
        readManyRows();
    }
}
