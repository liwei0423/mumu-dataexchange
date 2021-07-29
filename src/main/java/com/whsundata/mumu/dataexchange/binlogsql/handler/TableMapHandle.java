package com.whsundata.mumu.dataexchange.binlogsql.handler;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.whsundata.mumu.dataexchange.binlogsql.vo.DbInfoVo;

import java.util.Collections;
import java.util.List;

import static com.whsundata.mumu.dataexchange.binlogsql.tool.TableTool.setTableInfo;

/**
 * @description: 
 * @author: liwei
 * @date: 2021/7/29
 */
public class TableMapHandle implements BinlogEventHandle {
    private DbInfoVo dbInfoVo;

    public TableMapHandle(DbInfoVo dbInfoVo) {
        this.dbInfoVo = dbInfoVo;
    }


    @Override
    public List<String> handle(Event event, boolean isTurn) {
        TableMapEventData queryEventData = event.getData();
        setTableInfo(dbInfoVo, queryEventData);
        return Collections.emptyList();
    }

}
