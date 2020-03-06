package com.arb.clickhouse;

import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import javax.swing.plaf.nimbus.State;
import java.sql.*;
import java.util.UUID;
import java.util.logging.Logger;

public class ClickHouseClient {
    private ClickHouseDataSource dataSource;
    private Connection connection;
    private Logger log = Logger.getLogger(ClickHouseClient.class.getName());
    private static final String TABLE_NAME = "event";
    private static final String COLUMN_FAMILY = "event_info";
    private static final String ROW_KEY = "d6eb8287-afec-4854-bea1-cba988cb1d85";

    public ClickHouseClient() throws InterruptedException, SQLException {
        ClickHouseProperties properties = new ClickHouseProperties();
        dataSource = new ClickHouseDataSource("jdbc:clickhouse://192.168.1.225:8123", properties);
    }

    public void createTable() {
        try {
            connection = dataSource.getConnection();
            connection.createStatement().execute(
                    "DROP TABLE IF EXISTS " + TABLE_NAME
            );
            connection.createStatement().execute(
                    "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (rowKey String, event String, entityType String, entityId String, targetEntityType String, targetEntityId String, eventTime DateTime) ENGINE = TinyLog"
            );
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void insertOneRow() {
        PreparedStatement statement = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement("INSERT INTO " + TABLE_NAME +" (rowKey, event, entityType, entityId, targetEntityType, targetEntityId, eventTime) VALUES (?, ?, ?, ?, ?, ?, ?)");
            int userId = (int) Math.ceil(100 * Math.random());
            int itemId = (int) Math.ceil(1000 * Math.random());
            statement.setString(1, ROW_KEY);
            statement.setString(2, "view");
            statement.setString(3, "user");
            statement.setString(4, "" + userId);
            statement.setString(5, "item");
            statement.setString(6, "" + itemId);
            statement.setTimestamp(7, new Timestamp(System.currentTimeMillis()));

            long start = System.currentTimeMillis();
            statement.execute();
            long end = System.currentTimeMillis();

            connection.close();

            log.info("Row has been insert. Time - " + (end - start) + " ms.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void insertManyRows(int count) {
        count = 100000;
        PreparedStatement statement = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement("INSERT INTO " + TABLE_NAME +" (rowKey, event, entityType, entityId, targetEntityType, targetEntityId, eventTime) VALUES (?, ?, ?, ?, ?, ?, ?)");
            for ( int i = 0; i < count; i ++ ) {
                String rowKey = UUID.randomUUID().toString();
                int userId = (int) Math.ceil(100 * Math.random());
                int itemId = (int) Math.ceil(1000 * Math.random());
                statement.setString(1, rowKey);
                statement.setString(2, "view");
                statement.setString(3, "user");
                statement.setString(4, "" + userId);
                statement.setString(5, "item");
                statement.setString(6, "" + itemId);
                statement.setTimestamp(7, new Timestamp(System.currentTimeMillis()));
                statement.addBatch();
            }


            long start = System.currentTimeMillis();
            statement.executeBatch();
            long end = System.currentTimeMillis();

            connection.close();

            log.info("Rows has been insert. Count - " + count + ". Time - " + (end - start) + " ms.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void readOneRow(boolean displayReadData) {
        PreparedStatement statement = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement("SELECT * FROM " + TABLE_NAME + " WHERE rowKey = ?");
            statement.setString(1, ROW_KEY);

            long start = System.currentTimeMillis();
            ResultSet rs = statement.executeQuery();
            long end = System.currentTimeMillis();

            long count = 0;
            while (rs.next()) {
                count++;
            }

            connection.close();

            log.info("Rows has been read. Count - " + count + ". Time - " + (end - start) + " ms.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void readManyRows() {
        PreparedStatement statement = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement("SELECT * FROM " + TABLE_NAME);

            long start = System.currentTimeMillis();
            ResultSet rs = statement.executeQuery();
            long end = System.currentTimeMillis();

            long count = 0;
            while (rs.next()) {
                count++;
            }

            connection.close();

            log.info("Rows has been read. Count - " + count + ". Time - " + (end - start) + " ms.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void readCountRows() {
        PreparedStatement statement = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement("SELECT COUNT(*) as rowcount FROM " + TABLE_NAME);

            long start = System.currentTimeMillis();
            ResultSet rs = statement.executeQuery();
            long end = System.currentTimeMillis();

            rs.next();
            long count = rs.getLong("rowcount");

            connection.close();

            log.info("Count has been read. Count - " + count + ". Time - " + (end - start) + " ms.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void runAllTest() {
        createTable();
        insertOneRow();
        readOneRow(false);
        insertManyRows(60000);
        readManyRows();
        readCountRows();
    }
}
