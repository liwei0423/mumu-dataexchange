package com.whsundata.mumu.dataexchange.binlogsql.vo;

import lombok.Data;

import java.util.List;

@Data
public class TableVo {
    private String dbName;
    private String tableName;
    private List<ColumnVo> columns;

    public TableVo(String dbName, String tableName) {
        this.dbName = dbName;
        this.tableName = tableName;
    }
}
