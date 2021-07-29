package com.whsundata.mumu.dataexchange.sqlparser.visitor;


import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SelectColumnVisitor extends MySqlOutputVisitor {

    private Map<String, String> columnMap = new HashMap<>();

    public SelectColumnVisitor(Appendable appender) {
        super(appender);
    }

    @Override
    public boolean visit(MySqlSelectQueryBlock x) {
        List<SQLSelectItem> list = x.getSelectList();
        for (SQLSelectItem item : list) {
            SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) item.getExpr();
            columnMap.put(sqlPropertyExpr.getOwnerName(), sqlPropertyExpr.getSimpleName());
        }
        return true;
    }
}
