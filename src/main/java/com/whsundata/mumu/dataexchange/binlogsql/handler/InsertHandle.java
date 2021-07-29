package com.whsundata.mumu.dataexchange.binlogsql.handler;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.whsundata.mumu.dataexchange.binlogsql.Filter;
import com.whsundata.mumu.dataexchange.binlogsql.vo.RowVo;
import com.whsundata.mumu.dataexchange.binlogsql.vo.TableVo;

import java.util.Collections;
import java.util.List;

import static com.whsundata.mumu.dataexchange.binlogsql.tool.SqlGenerateTool.*;
import static com.whsundata.mumu.dataexchange.binlogsql.tool.TableTool.getTableInfo;

/**
 * @description: 
 * @author: liwei
 * @date: 2021/7/29
 */
public class InsertHandle implements BinlogEventHandle {

    private final Filter filter;

    public InsertHandle(Filter filter) {
        this.filter = filter;
    }

    @Override
    public List<String> handle(Event event, boolean isTurn) {
        WriteRowsEventData writeRowsEventV2 = event.getData();

        TableVo tableVoInfo = getTableInfo(writeRowsEventV2.getTableId());

        if (!filter.filter(tableVoInfo)) {
            return Collections.emptyList();
        }

        List<RowVo> rows = changeToRowVo(tableVoInfo, writeRowsEventV2.getRows());
        if (isTurn) {
            return deleteSql(tableVoInfo, rows, getComment(event.getHeader()));
        } else {
            return insertSql(tableVoInfo, rows, getComment(event.getHeader()));
        }

    }


}
