package com.whsundata.mumu.dataexchange.canal;

import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.Message;

import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.*;

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
            handleMessage(message);
        }
        connector.ack(batchId);
    }

    private static void handleMessage(Message message) {
        List<Entry> entries = message.getEntries();
        for (Entry entry : entries) {
            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }
//            printEntry(entry);
            String sql = "SELECT * FROM user t1 inner join user_info t2 on t1.user_no = t2.user_no where t2.user_no = '11'";
            Map<String, Object> parms = new HashMap<>();
            try {
                List<Entity> list = Db.use().query(sql, parms);
                for (Entity entity : list) {
                    Set<String> set = entity.getFieldNames();
                    Iterator<String> iterator = set.iterator();
                    while (iterator.hasNext()) {
                        String filedName = iterator.next();
                        System.out.println(filedName + " --> " + entity.getStr(filedName));
                    }
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
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
                System.out.println("-------&gt; before");
                printColumn(rowData.getBeforeColumnsList());
                System.out.println("-------&gt; after");
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
