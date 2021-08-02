package com.whsundata.mumu.dataexchange.binlogsql;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.whsundata.mumu.dataexchange.binlogsql.handler.BinlogEventHandle;
import com.whsundata.mumu.dataexchange.test.KafkaProducer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Slf4j
public class BinlogParser {

    @Getter
    private Map<EventType, BinlogEventHandle> handleRegisterMap = new HashMap<>();

    public void registerHandle(BinlogEventHandle handle, EventType... eventTypes) {
        for (EventType eventType : eventTypes) {
            handleRegisterMap.put(eventType, handle);
        }
    }

    public void handle(Event event) {
        BinlogEventHandle binlogEventHandle = handleRegisterMap.get(event.getHeader().getEventType());
        if (binlogEventHandle != null) {
            List<String> sql = binlogEventHandle.handle(event, false);
            if (!sql.isEmpty()) {
                log.info("handle sql: " + sql);
                String topic = "dev-test-user";
                KafkaProducer.send(topic, "user", sql.get(0));
            }
        }
    }
}
