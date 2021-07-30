package com.whsundata.mumu.dataexchange.canal;

import cn.hutool.json.JSONObject;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.Message;
import com.whsundata.mumu.dataexchange.dataset.sink.SinkMessage;
import com.whsundata.mumu.dataexchange.dataset.source.DatasetSql;
import com.whsundata.mumu.dataexchange.db.DbTool;
import com.whsundata.mumu.dataexchange.sqlparser.SqlParser;
import com.whsundata.mumu.dataexchange.vo.MessageVO;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @description: canal客户端
 * @author: liwei
 * @date: 2021/7/29
 */
public class CanalClient {

    public static void execute() {
        String ip = "10.0.40.231";
        String username = "canal";
        String password = "canal";
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(ip,
                11111), "example", "canal", "canal");

        connector.connect();
        connector.subscribe(".*\\..*");
        connector.rollback();
        Message message = connector.getWithoutAck(1);
        long batchId = message.getId();
        int size = message.getEntries().size();
        if (batchId == -1 || size == 0) {
        } else {
            try {
                handleMessage(message);
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        connector.ack(batchId);
    }

    /**
     * @description: 处理canal消息
     */
    private static void handleMessage(Message message) throws SQLException, IOException {
        List<Entry> entries = message.getEntries();
        for (Entry entry : entries) {
            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }
            printEntry(entry);
            MessageVO messageVO = parseMessage(entry);
            String sql = DatasetSql.getSql();
            SQLSelectStatement statement = SqlParser.getStatement(sql);
            Map<String, String> tableNameMap = SqlParser.getTableNameMap(statement);
            Map<String, String> selectColumnMap = SqlParser.getSelectColumnMap(statement);
            String newSql = SqlParser.addCondition(statement, messageVO);
            System.out.println("################newsql:\r\n" + newSql);
            List<Map<String, String>> resultList = DbTool.query(newSql);
            SinkMessage sinkMessage = new SinkMessage("user2", resultList);
            JSONObject jsonObject = new JSONObject(sinkMessage);
            System.out.println("jsonstr=" + jsonObject.toString());
        }
    }

    /**
     * @description: 解析消息数据
     */
    private static MessageVO parseMessage(Entry entry) {
        MessageVO messageVO = new MessageVO();
        CanalEntry.RowChange rowChage;
        try {
            rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
        } catch (Exception e) {
            throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                    e);
        }
        String tableName = entry.getHeader().getTableName();
        messageVO.setTableName(tableName);
        CanalEntry.EventType eventType = rowChage.getEventType();
        for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
            if (eventType == CanalEntry.EventType.DELETE) {
                System.out.println("不支持 delete");
            } else {
                getMessageVO(messageVO, rowData.getAfterColumnsList());
            }
        }
        return messageVO;
    }

    private static void getMessageVO(MessageVO messageVO, List<CanalEntry.Column> columnList) {
        Map<String, String> primarykeyMap = new HashMap<>();
        Map<String, CanalEntry.Column> rowDataMap = new LinkedHashMap<>();
        for (CanalEntry.Column item : columnList) {
            rowDataMap.put(item.getName(), item);
            if (item.getIsKey()) {
                primarykeyMap.put(item.getName(), item.getValue());
            }
        }
        messageVO.setPrimarykeyMap(primarykeyMap);
        messageVO.setRowDataMap(rowDataMap);
    }

    private static void printEntry(Entry entry) {
        CanalEntry.RowChange rowChage = null;
        try {
            rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
        } catch (Exception e) {
            throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                    e);
        }

        CanalEntry.EventType eventType = rowChage.getEventType();
        System.out.println(String.format("================&gt; binlog[%s:%s] , name[%s,%s] , eventType : %s",
                entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                eventType));

        for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
            if (eventType == CanalEntry.EventType.DELETE) {
                printColumn(rowData.getBeforeColumnsList());
            } else if (eventType == CanalEntry.EventType.INSERT) {
                printColumn(rowData.getAfterColumnsList());
            } else {
                printColumn(rowData.getAfterColumnsList());
            }
        }
    }

    private static void printColumn(List<CanalEntry.Column> columns) {
        for (CanalEntry.Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated() + "    iskey=" + column.getIsKey());
        }
    }

    public static void main(String[] args) {
        execute();
    }
}
