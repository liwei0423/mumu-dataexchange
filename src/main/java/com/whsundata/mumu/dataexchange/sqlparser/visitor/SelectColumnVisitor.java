package com.whsundata.mumu.dataexchange.sqlparser.visitor;


import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @description: 解析sql查询结果集
 * @author: liwei
 * @date: 2021/7/29
 */
public class SelectColumnVisitor extends MySqlOutputVisitor {

    public Map<String, String> selectColumnMap = new HashMap<>();

    public SelectColumnVisitor(Appendable appender) {
        super(appender);
    }

    @Override
    public boolean visit(MySqlSelectQueryBlock x) {
        List<SQLSelectItem> list = x.getSelectList();
        for (SQLSelectItem item : list) {
            SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) item.getExpr();
            selectColumnMap.put(sqlPropertyExpr.getOwnerName(), sqlPropertyExpr.getSimpleName());
        }
        return true;
    }
}
