package com.whsundata.mumu.dataexchange.test;

import cn.hutool.db.Db;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TestDb {

    @Test
    public void meta() throws SQLException {
        Connection connection = Db.use().getConnection();
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getPrimaryKeys("test1", null, "user");
        while (rs.next()) {
            System.out.println(rs.getString("COLUMN_NAME"));
        }
        ResultSet columns = metaData.getColumns("test1", null, "user", null);
        while(columns.next()){

        }
    }
}
