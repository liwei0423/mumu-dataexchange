package com.whsundata.mumu.dataexchange.binlogsql.handler;

import com.github.shyiko.mysql.binlog.event.Event;

import java.util.List;


public interface BinlogEventHandle {

    List<String> handle(Event event, boolean isTurn);
}
