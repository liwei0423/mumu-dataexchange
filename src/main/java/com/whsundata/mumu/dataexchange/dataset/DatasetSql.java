package com.whsundata.mumu.dataexchange.dataset;

import java.sql.SQLSyntaxErrorException;

/**
 * @description: SQL数据集
 * @author: liwei
 * @date: 2021/7/29
 */
public class DatasetSql {

    public static String getSql() throws SQLSyntaxErrorException {
        String sql = "SELECT t1.user_id,t2.user_no FROM user t1 inner join user_info t2 on t1.user_no = t2.user_no where t2.user_no = '11'";
        return sql;
    }

}
