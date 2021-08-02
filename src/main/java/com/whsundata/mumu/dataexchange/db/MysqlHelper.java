package com.whsundata.mumu.dataexchange.db;

import cn.hutool.db.Db;
import cn.hutool.json.JSONUtil;
import com.whsundata.mumu.dataexchange.dataset.sink.SinkMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @description:
 * @author: liwei
 * @date: 2021/8/2
 */
@Slf4j
@Component
public class MysqlHelper {

    public static void batchPrepareInsert(SinkMessage sinkMessage) throws SQLException {
        List<Map<String, String>> dataList = sinkMessage.getDataList();
        Map<String, String> row = dataList.get(0);
        int columnSize = row.size();
        StringBuilder sqlPlaceHolder = new StringBuilder();
        for (int i = 1; i <= columnSize; i++) {
            if (i == columnSize) {
                sqlPlaceHolder.append("?");
            } else {
                sqlPlaceHolder.append("?,");
            }
        }
        String sql = String.format("insert into `%s`.`%s` (%s) values (%s)",
                "test1",
                "user2",
                StringUtils.join(dataList.get(0).keySet(), ", "),
                sqlPlaceHolder.toString()
        );
        Connection connection = Db.use().getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        for (Map<String, String> item : dataList) {
            int index = 1;
            for (String key : item.keySet()) {
                preparedStatement.setObject(index++, item.get(key));
            }
            preparedStatement.addBatch();
        }
        int[] batchResult = preparedStatement.executeBatch();
        int updateCount = Arrays.stream(batchResult).sum();
        preparedStatement.close();
        log.info("处理[batchExecutePrepareInsert]总数:{},受影响行数:{}", dataList.size(), updateCount);
    }

    public static void batchPrepareUpdate(SinkMessage sinkMessage) throws SQLException {
        Map<String, String> primaryKey = sinkMessage.getPrimaryKey();
        List<Map<String, String>> dataList = sinkMessage.getDataList();
        Map<String, String> row = dataList.get(0);
        int columnSize = row.size();
        StringBuilder updateSb = new StringBuilder();
        for (String key : row.keySet()) {
            if (!primaryKey.containsKey(key)) {
                updateSb.append(key + "=?,");
            }
        }
        String updateStr = StringUtils.removeEnd(updateSb.toString(), ",");

        StringBuilder whereSb = new StringBuilder();
        for (String key : primaryKey.keySet()) {
            whereSb.append(key + "=? and ");
        }
        String whereStr = StringUtils.removeEnd(whereSb.toString(), "and ");

        String sql = String.format("update `%s`.`%s` set %s where %s",
                "test1",
                "user2",
                updateStr,
                whereStr
        );
        log.info("sql=" + sql);
        Connection connection = Db.use().getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        for (Map<String, String> item : dataList) {
            int index = 1;
            for (String key : item.keySet()) {
                if (!primaryKey.containsKey(key)) {
                    preparedStatement.setObject(index++, item.get(key));
                }
            }
            for (String key : item.keySet()) {
                if (primaryKey.containsKey(key)) {
                    preparedStatement.setObject(index++, item.get(key));
                }
            }
            preparedStatement.addBatch();
        }
        int[] batchResult = preparedStatement.executeBatch();
        int updateCount = Arrays.stream(batchResult).sum();
        preparedStatement.close();
        log.info("处理[batchExecutePrepareInsert]总数:{},受影响行数:{}", dataList.size(), updateCount);
    }

    public static void batchPrepareDelete(SinkMessage sinkMessage) throws SQLException {
        Map<String, String> primaryKey = sinkMessage.getPrimaryKey();
        List<Map<String, String>> dataList = sinkMessage.getDataList();
        Map<String, String> row = dataList.get(0);
        int columnSize = row.size();
        StringBuilder whereSb = new StringBuilder();
        for (String key : primaryKey.keySet()) {
            whereSb.append(key + "=? and ");
        }
        String whereStr = StringUtils.removeEnd(whereSb.toString(), "and ");

        String sql = String.format("delete from `%s`.`%s` where %s",
                "test1",
                "user2",
                whereStr
        );
        log.info("sql=" + sql);
        Connection connection = Db.use().getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        for (Map<String, String> item : dataList) {
            int index = 1;
            for (String key : item.keySet()) {
                if (primaryKey.containsKey(key)) {
                    preparedStatement.setObject(index++, item.get(key));
                }
            }
            preparedStatement.addBatch();
        }
        int[] batchResult = preparedStatement.executeBatch();
        int updateCount = Arrays.stream(batchResult).sum();
        preparedStatement.close();
        log.info("处理[batchExecutePrepareInsert]总数:{},受影响行数:{}", dataList.size(), updateCount);
    }

    public static void main(String[] args) {
        String data = "{\"tableName\":\"user2\",\"dataList\":[{\"address\":\"湖北\",\"user_name\":\"李1\",\"user_no\":\"11\",\"phone\":\"13811111111\"},{\"address\":\"深圳\",\"user_name\":\"李11\",\"user_no\":\"12\",\"phone\":\"13855555555\"}],\"sinkType\":\"u\",\"primaryKey\":{\"user_no\":\"1\"}}";
        SinkMessage sinkMessage = JSONUtil.toBean(data, SinkMessage.class);
        try {
//            MysqlHelper.batchPrepareInsert(sinkMessage);
//            MysqlHelper.batchPrepareUpdate(sinkMessage);
            MysqlHelper.batchPrepareDelete(sinkMessage);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        System.out.println("---");
    }
}
