package com.whsundata.mumu.dataexchange.binlogsql.handler;

import com.github.shyiko.mysql.binlog.event.Event;

import java.util.function.Predicate;

/**
 * @description:
 * @author: liwei
 * @date: 2021/7/29
 */
public interface EventFilter extends Predicate<Event> {
    @Override
    boolean test(Event event);
}
