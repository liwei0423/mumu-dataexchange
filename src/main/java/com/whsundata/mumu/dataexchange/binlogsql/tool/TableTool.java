package com.whsundata.mumu.dataexchange.binlogsql.tool;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import com.whsundata.mumu.dataexchange.binlogsql.vo.ColumnVo;
import com.whsundata.mumu.dataexchange.binlogsql.vo.DbInfoVo;
import com.whsundata.mumu.dataexchange.binlogsql.vo.TableVo;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TableTool {

    public static Map<Long, TableVo> tableInfoMap = new HashMap<>();

    public static TableVo getTableInfo(Long tableId) {
        if (tableInfoMap.containsKey(tableId)) {
            return tableInfoMap.get(tableId);
        }
        throw new RuntimeException("出现异常, 没找到tableId");
    }

    public static void setTableInfo(DbInfoVo dbInfoVo, TableMapEventData queryEventData) {
        String tableName = queryEventData.getTable();
        String dbName = queryEventData.getDatabase();
        long tableId = queryEventData.getTableId();

        if (tableInfoMap.containsKey(queryEventData.getTableId())) {
            return;
        }
        String url = "jdbc:mysql://" + dbInfoVo.getHost() + ":" + dbInfoVo.getPort() + "/" + dbName + "?characterEncoding=UTF-8&autoReconnect=true";

        try (Connection connection = DriverManager.getConnection(url, dbInfoVo.getUsername(), dbInfoVo.getPassword())) {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet columns = metaData.getColumns(dbName, null, tableName, null);
            TableVo tableVo = new TableVo(dbName, tableName);

            List<ColumnType> columnTypes = new ArrayList<>();
            for (byte columnType : queryEventData.getColumnTypes()) {
                columnTypes.add(ColumnType.byCode(columnType));
            }

            List<ColumnVo> columnVos = new ArrayList<>();
            int i = 0;
            while (columns.next()) {
                String column = columns.getString("COLUMN_NAME");
                columnVos.add(new ColumnVo(column,
                        ColumnType.byCode(queryEventData.getColumnTypes()[i] & 0xFF),
                        JDBCType.valueOf(columns.getInt("DATA_TYPE"))));
                i++;
            }
            tableVo.setColumns(columnVos);
            tableInfoMap.put(tableId, tableVo);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
