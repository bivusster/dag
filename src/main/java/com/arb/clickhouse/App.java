package com.arb.clickhouse;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.apache.commons.beanutils.converters.SqlDateConverter;
import ru.yandex.clickhouse.ClickHouseArray;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.domain.ClickHouseDataType;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

public class App {
	
    public static void main( String[] args ) throws SQLException, InterruptedException {
        ClickHouseClient cClient = new ClickHouseClient();
        cClient.runAllTest();

        //HBaseClient hClient = new HBaseClient();
        //hClient.runAllTest();
    }
}
