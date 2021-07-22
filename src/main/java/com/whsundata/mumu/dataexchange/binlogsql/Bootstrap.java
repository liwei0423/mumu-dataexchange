package com.whsundata.mumu.dataexchange.binlogsql;

import com.whsundata.mumu.dataexchange.binlogsql.vo.CommonFilter;
import com.whsundata.mumu.dataexchange.binlogsql.vo.DbInfoVo;
import com.whsundata.mumu.dataexchange.binlogsql.vo.FilterDbTableVo;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;


@Slf4j
public class Bootstrap {
    public static void main(String[] args) throws Exception {
        log.info("#############");
        DbInfoVo dbInfoVo = new DbInfoVo();
        dbInfoVo.setHost("10.0.40.231");
        dbInfoVo.setPort(3306);
        dbInfoVo.setUsername("binlog");
        dbInfoVo.setPassword("binlog");
        List<FilterDbTableVo> filterDbTableVos = new ArrayList<>();
        FilterDbTableVo filterDbTableVo = new FilterDbTableVo();
        filterDbTableVo.setDbName("test1");
        filterDbTableVo.setTableName("user");
        filterDbTableVos.add(filterDbTableVo);
        new BinlogListenSql(dbInfoVo)
                .setFilter(new CommonFilter().setStartTime(System.currentTimeMillis())
                .setIncludeDbTableVos(filterDbTableVos))
                .connectAndListen();
    }
}
