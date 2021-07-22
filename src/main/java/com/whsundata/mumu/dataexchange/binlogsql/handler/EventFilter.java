package com.whsundata.mumu.dataexchange.binlogsql.handler;

import com.github.shyiko.mysql.binlog.event.Event;

import java.util.function.Predicate;


public interface EventFilter extends Predicate<Event> {
    @Override
    boolean test(Event event);
}
