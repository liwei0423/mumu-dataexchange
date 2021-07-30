package com.whsundata.mumu.dataexchange.sqlparser.visitor;

import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;

import java.util.Map;

/**
 * @description: sql重组增加查询条件
 * @author: liwei
 */
public class AddConditionVisitor extends MySqlOutputVisitor {

    Map<String, Object> condition;

    public AddConditionVisitor(Appendable appender, Map<String, Object> condition) {
        super(appender);
        this.condition = condition;
    }

    @Override
    public boolean visit(SQLSelectStatement stmt) {
        SQLSelectQueryBlock queryBlock = stmt.getSelect().getQueryBlock();
        for (String key : condition.keySet()) {
            queryBlock.addCondition(key + "=" + condition.get(key));
        }
        return true;
    }
}
