package com.whsundata.mumu.dataexchange.db;

import cn.hutool.db.Db;
import cn.hutool.db.Entity;

import java.sql.SQLException;
import java.util.*;

/**
 * @description: 数据库工具类
 * @author: liwei
 * @date: 2021/7/30
 */
public class DbTool {

    /**
     * @description: sql查询结果集
     */
    public static List<Map<String, String>> query(String sql) throws SQLException {
        List<Map<String, String>> resultList = new ArrayList<>();
        Map<String, String> rowMap;
        List<Entity> list = Db.use().query(sql, new HashMap<>());
        for (Entity entity : list) {
            rowMap = new LinkedHashMap<>();
            Set<String> set = entity.getFieldNames();
            Iterator<String> iterator = set.iterator();
            while (iterator.hasNext()) {
                String fieldName = iterator.next();
                System.out.println(fieldName + " ---> " + entity.getStr(fieldName));
                rowMap.put(fieldName, entity.getStr(fieldName));
            }
            resultList.add(rowMap);
        }
        return resultList;
    }

}
