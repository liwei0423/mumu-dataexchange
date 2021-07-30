package com.whsundata.mumu.dataexchange.sqlparser;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.whsundata.mumu.dataexchange.sqlparser.common.SqlTypeUtil;
import com.whsundata.mumu.dataexchange.sqlparser.visitor.AddConditionVisitor;
import com.whsundata.mumu.dataexchange.sqlparser.visitor.SelectColumnVisitor;
import com.whsundata.mumu.dataexchange.sqlparser.visitor.TableNameVisitor;
import com.whsundata.mumu.dataexchange.vo.MessageVO;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.SQLSyntaxErrorException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @description: sql解析器
 * @author: liwei
 * @date: 2021/7/29
 */
public class SqlParser {

    /**
     * @description: 解析sql
     */
    public static SQLStatement parser(String sql, DbType dbType) throws SQLSyntaxErrorException {
        List<SQLStatement> list = SQLUtils.parseStatements(sql, dbType);
        if (list.size() > 1) {
            throw new SQLSyntaxErrorException("MultiQueries is not supported,use single query instead ");
        }
        return list.get(0);
    }

    /**
     * @description: 解析sql
     */
    public static SQLSelectStatement getStatement(String sql) throws SQLSyntaxErrorException {
        SQLSelectStatement statement = (SQLSelectStatement) parser(sql, DbType.mysql);
        return statement;
    }

    /**
     * @description: 获取sql中的表信息
     */
    public static Map<String, String> getTableNameMap(SQLSelectStatement statement) throws IOException {
        StringWriter out = new StringWriter();
        TableNameVisitor tableNameVisitor = new TableNameVisitor(out);
        tableNameVisitor.visit(statement);
        out.close();
        return tableNameVisitor.tableNameMap;
    }

    /**
     * @description: 获取sql查询结果列
     */
    public static Map<String, String> getSelectColumnMap(SQLSelectStatement statement) throws IOException {
        StringWriter out = new StringWriter();
        SelectColumnVisitor selectColumnVisitor = new SelectColumnVisitor(out);
        selectColumnVisitor.visit(statement);
        out.close();
        return selectColumnVisitor.selectColumnMap;
    }

    /**
     * @description: sql重组增加查询条件
     */
    private static String addCondition(SQLSelectStatement statement, Map<String, Object> condition) {
        StringWriter out = new StringWriter();
        AddConditionVisitor addConditionVisitor = new AddConditionVisitor(out, condition);
        addConditionVisitor.visit(statement);
        return statement.toString();
    }

    /**
     * @description: sql重组增加查询条件
     */
    public static String addCondition(SQLSelectStatement statement, MessageVO messageVO) throws IOException {
        Map<String, String> tableNameMap = getTableNameMap(statement);
        String tableAlias = getTableAlias(tableNameMap, messageVO.getTableName());
        Map<String, Object> condition = new HashMap<>();
        Map<String, CanalEntry.Column> rowDataMap = messageVO.getRowDataMap();
        for (String key : rowDataMap.keySet()) {
            CanalEntry.Column column = rowDataMap.get(key);
            condition.put(tableAlias + "." + key, SqlTypeUtil.getStringByColumnValue(column));
        }
        return addCondition(statement, condition);
    }

    private static String getTableAlias(Map<String, String> tableNameMap, String tableName) {
        if (StringUtils.isBlank(tableName)) {
            return StringUtils.EMPTY;
        }
        for (String key : tableNameMap.keySet()) {
            if (tableName.equals(tableNameMap.get(key))) {
                return key;
            }
        }
        return StringUtils.EMPTY;
    }
}
