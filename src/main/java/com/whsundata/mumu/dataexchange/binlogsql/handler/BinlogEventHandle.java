package com.whsundata.mumu.dataexchange.binlogsql.handler;

import com.github.shyiko.mysql.binlog.event.Event;

import java.util.List;

/**
 * @description:
 * @author: liwei
 * @date: 2021/7/29
 */
public interface BinlogEventHandle {

    List<String> handle(Event event, boolean isTurn);
}
