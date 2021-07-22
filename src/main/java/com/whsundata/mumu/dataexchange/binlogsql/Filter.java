package com.whsundata.mumu.dataexchange.binlogsql;

import com.github.shyiko.mysql.binlog.event.Event;
import com.whsundata.mumu.dataexchange.binlogsql.vo.TableVo;


public interface Filter {
    default boolean filter(TableVo tableVoInfo) {
        return true;
    }

    default boolean filter(Event event) {
        return true;
    }
}
