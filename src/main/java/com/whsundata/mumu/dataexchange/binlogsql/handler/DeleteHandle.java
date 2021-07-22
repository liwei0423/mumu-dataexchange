package com.whsundata.mumu.dataexchange.binlogsql.handler;

import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.whsundata.mumu.dataexchange.binlogsql.Filter;
import com.whsundata.mumu.dataexchange.binlogsql.vo.RowVo;
import com.whsundata.mumu.dataexchange.binlogsql.vo.TableVo;

import java.util.Collections;
import java.util.List;

import static com.whsundata.mumu.dataexchange.binlogsql.tool.SqlGenerateTool.*;
import static com.whsundata.mumu.dataexchange.binlogsql.tool.TableTool.getTableInfo;


public class DeleteHandle implements BinlogEventHandle {

    private final Filter filter;

    public DeleteHandle(Filter filter) {
        this.filter = filter;
    }

    @Override
    public List<String> handle(Event event, boolean isTurn) {
        DeleteRowsEventData deleteRowsEventData = event.getData();
        TableVo tableVoInfo = getTableInfo(deleteRowsEventData.getTableId());

        if (!filter.filter(tableVoInfo)) {
            return Collections.emptyList();
        }

        List<RowVo> rows = changeToRowVo(tableVoInfo, deleteRowsEventData.getRows());

        if (isTurn) {
            return insertSql(tableVoInfo, rows, getComment(event.getHeader()));
        } else {
            return deleteSql(tableVoInfo, rows, getComment(event.getHeader()));
        }
    }

}
