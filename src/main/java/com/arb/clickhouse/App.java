package com.arb.clickhouse;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import ru.yandex.clickhouse.ClickHouseArray;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.domain.ClickHouseDataType;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

public class App {
	
    public static void main( String[] args ) throws SQLException, InterruptedException {
        System.out.println( "Start clickhouse" );
        DAL dal = setup();
        
        dal.connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS event (event String, entityType String, entityId String, targetEntityType String, targetEntityId String, eventTime DateTime) ENGINE = TinyLog"
        );

        
        PreparedStatement statement = dal.connection.prepareStatement("INSERT INTO event (event, entityType, entityId, targetEntityType, targetEntityId, eventTime) VALUES (?, ?, ?, ?, ?, ?)");

        for ( int i = 0; i < 1000; i ++ ) {
	        statement.setString(1, "view");
	        statement.setString(2, "user");
	        int userId = (int) Math.ceil(100 * Math.random());
	        statement.setString(3, "x" + userId);
	        statement.setString(4, "item");
	        int itemId = (int) Math.ceil(1000 * Math.random());
	        statement.setString(5, "i" + itemId);
	        statement.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
	        statement.addBatch();
	        Thread.sleep(10);
        }
//        
//        statement.setInt(1, 2);
//        statement.setArray(2, new ClickHouseArray(ClickHouseDataType.Int32, new int[]{2, 3, 4, 5}));
//        statement.addBatch();
//        statement.executeBatch();
        
//        ResultSet rs = dal.connection.createStatement().executeQuery(
//                "SELECT * FROM test.arrau");
//        while(rs.next()) {
//        	System.out.println(rs.getInt(1));
//        }
            
        System.out.println( "Stop clickhouse" );
        dal.close();
    }
    
    private static DAL setup() throws SQLException {
        DAL result = new DAL();
        ClickHouseProperties properties = new ClickHouseProperties();
        result.dataSource = new ClickHouseDataSource("jdbc:clickhouse://192.168.1.225:8123", properties);
        result.connection = result.dataSource.getConnection();
        result.connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
        return result;
    }
    
    private static class DAL {
        ClickHouseDataSource dataSource;
        Connection connection;
        void close() throws SQLException {
            if (null != connection) connection.close();
        }
    }
}
