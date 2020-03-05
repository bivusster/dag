package com.arb.clickhouse;

import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.UUID;
import java.util.logging.Logger;

public class ClickHouseClient {
    private ClickHouseDataSource dataSource;
    private Connection connection;
    private Logger log = Logger.getLogger(ClickHouseClient.class.getName());
    private static final String TABLE_NAME = "event";
    private static final String COLUMN_FAMILY = "event_info";
    private static final String ROW_KEY = "d6eb8287-afec-4854-bea1-cba988cb1d85";

    private ClickHouseClient() throws InterruptedException, SQLException {
        ClickHouseProperties properties = new ClickHouseProperties();
        dataSource = new ClickHouseDataSource("jdbc:clickhouse://192.168.1.225:8123", properties);
    }

    private void createTable() {
        try {
            connection = dataSource.getConnection();
            connection.createStatement().execute(
                    "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (event String, entityType String, entityId String, targetEntityType String, targetEntityId String, eventTime DateTime) ENGINE = TinyLog"
            );
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void insertManyRows(int count) {
        PreparedStatement statement = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement("INSERT INTO " + TABLE_NAME +" (event, entityType, entityId, targetEntityType, targetEntityId, eventTime) VALUES (?, ?, ?, ?, ?, ?)");
            for ( int i = 0; i < count; i ++ ) {
                int userId = (int) Math.ceil(100 * Math.random());
                int itemId = (int) Math.ceil(1000 * Math.random());
                statement.setString(1, "view");
                statement.setString(2, "user");
                statement.setString(3, "" + userId);
                statement.setString(4, "item");
                statement.setString(5, "" + itemId);
                statement.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
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

    public void runAllTest() {

    }
}
