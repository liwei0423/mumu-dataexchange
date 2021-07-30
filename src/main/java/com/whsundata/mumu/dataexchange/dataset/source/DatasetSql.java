package com.whsundata.mumu.dataexchange.dataset.source;

import java.sql.SQLSyntaxErrorException;

/**
 * @description: SQL数据集
 * @author: liwei
 * @date: 2021/7/29
 */
public class DatasetSql {

    public static String getSql() throws SQLSyntaxErrorException {
        String sql = "SELECT t1.user_no,t1.user_name,t2.phone,t2.address FROM user t1 inner join user_info t2 on t1.user_id = t2.user_id";
        return sql;
    }

}
