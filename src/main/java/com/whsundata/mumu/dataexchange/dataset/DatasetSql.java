package com.whsundata.mumu.dataexchange.dataset;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.whsundata.mumu.dataexchange.sqlparser.visitor.SelectColumnVisitor;
import com.whsundata.mumu.dataexchange.sqlparser.visitor.TableNameVisitor;

import java.io.StringWriter;
import java.sql.SQLSyntaxErrorException;
import java.util.List;

public class DatasetSql {

    public static void main(String[] args) throws SQLSyntaxErrorException {
        String sql = "SELECT t1.user_id,t2.user_no FROM user t1 inner join user_info t2 on t1.user_no = t2.user_no where t2.user_no = '11'";
        String dbType = "mysql";
        System.out.println("原始SQL 为 ： " + sql);
        SQLSelectStatement statement = (SQLSelectStatement) parser(sql, dbType);
        StringWriter out = new StringWriter();
        TableNameVisitor tableNameVisitor = new TableNameVisitor(out);
        tableNameVisitor.visit(statement);
        SelectColumnVisitor selectColumnVisitor = new SelectColumnVisitor(out);
        selectColumnVisitor.visit(statement);

    }

    public static SQLStatement parser(String sql, String dbType) throws SQLSyntaxErrorException {
        List<SQLStatement> list = SQLUtils.parseStatements(sql, dbType);
        if (list.size() > 1) {
            throw new SQLSyntaxErrorException("MultiQueries is not supported,use single query instead ");
        }
        return list.get(0);
    }
}
