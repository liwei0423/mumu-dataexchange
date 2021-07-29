package com.whsundata.mumu.dataexchange.canal;

import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.Message;
import com.whsundata.mumu.dataexchange.vo.MessageVO;

import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.*;

/**
 * @description:
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
            handleMessage(message);
        }
        connector.ack(batchId);
    }

    /**
     * @description: 处理canal消息
     */
    private static void handleMessage(Message message) {
        List<Entry> entries = message.getEntries();
        for (Entry entry : entries) {
            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }
//            printEntry(entry);
            MessageVO messageVO = parseMessage(entry);
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
