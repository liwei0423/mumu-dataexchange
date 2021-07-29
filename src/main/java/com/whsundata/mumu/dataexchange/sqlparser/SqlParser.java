package com.whsundata.mumu.dataexchange.sqlparser;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;

import java.sql.SQLSyntaxErrorException;
import java.util.List;

/**
 * @description: sql解析器
 * @author: liwei
 * @date: 2021/7/29
 */
public class SqlParser {

    /**
     * @param dbType 数据库类型
     * @description: 解析sql
     */
    public static SQLStatement parser(String sql, DbType dbType) throws SQLSyntaxErrorException {
        List<SQLStatement> list = SQLUtils.parseStatements(sql, dbType);
        if (list.size() > 1) {
            throw new SQLSyntaxErrorException("MultiQueries is not supported,use single query instead ");
        }
        return list.get(0);
    }
}
