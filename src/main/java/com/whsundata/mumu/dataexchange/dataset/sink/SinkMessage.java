package com.whsundata.mumu.dataexchange.dataset.sink;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * @description: 数据消息封装类
 * @author: liwei
 * @date: 2021/7/30
 */
@Data
public class SinkMessage {

    private String database;
    private String tableName;
    private List<Map<String, String>> dataList;
    private String sinkType;
    private Map<String, String> primaryKey;

    public SinkMessage(String tableName, List<Map<String, String>> dataList, Map<String, String> primaryKey) {
        this.tableName = tableName;
        this.dataList = dataList;
        this.primaryKey = primaryKey;
    }
}
