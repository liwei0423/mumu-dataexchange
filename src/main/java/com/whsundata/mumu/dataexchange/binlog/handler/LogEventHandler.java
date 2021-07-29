package com.whsundata.mumu.dataexchange.binlog.handler;

import com.alibaba.fastjson.JSON;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogClient.EventListener;
import com.github.shyiko.mysql.binlog.event.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.Future;

/**
 * @description:
 * @author: liwei
 * @date: 2021/7/29
 */
@Component("eventListener")
public class LogEventHandler implements EventListener {

    @Autowired
    BinaryLogClient binaryLogClient;

    @Override
    public void onEvent(Event event) {
        EventHeader header = event.getHeader();
        EventType eventType = header.getEventType();
        System.out.println("监听的事件类型:" + eventType);
        if (EventType.isWrite(eventType)) {
            //获取事件体
            WriteRowsEventData data = event.getData();
            System.out.println(JSON.toJSONString(data));
        } else if (EventType.isUpdate(eventType)) {
            UpdateRowsEventData data = event.getData();
            System.out.println(JSON.toJSONString(data));
        } else if (EventType.isDelete(eventType)) {
            DeleteRowsEventData data = event.getData();
            System.out.println(JSON.toJSONString(data));
        } else if (event.getHeader().getEventType() == EventType.QUERY) {
            QueryEventData data = event.getData();
            System.out.println(data.toString());  // 这行有惊喜
            System.out.println("--" + data.getSql());
        }

    }

    @Async
    public Future<String> start() {
        try {
            binaryLogClient.registerEventListener(this);
            binaryLogClient.connect();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new AsyncResult<String>("start ok");
    }

    @Async
    public Future<String> stop() {
        try {
            binaryLogClient.unregisterEventListener(this);
            binaryLogClient.disconnect();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new AsyncResult<String>("stop ok");
    }
}
