package com.whsundata.mumu.dataexchange.sqlparser.visitor;

import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;

import java.util.HashMap;
import java.util.Map;

public class TableNameVisitor extends MySqlOutputVisitor {

    private Map<String, String> tableMap = new HashMap<>();

    public TableNameVisitor(Appendable appender) {
        super(appender);
    }

    @Override
    public boolean visit(SQLExprTableSource x) {
        SQLName sqlName = (SQLName) x.getExpr();
        String alias = x.getAlias();
        String tableName = sqlName.getSimpleName();
        tableMap.put(alias, tableName);
        return true;
    }

}