package com.whsundata.mumu.dataexchange.sqlparser.common;

import com.alibaba.otter.canal.protocol.CanalEntry;

import javax.xml.bind.DatatypeConverter;
import java.sql.JDBCType;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * @description: sql类型转换工具类
 * @author: liwei
 * @date: 2021/7/30
 */
public class SqlTypeUtil {

    public static final ZoneId UTC = ZoneId.of("UTC");
    public static DateTimeFormatter dateTimeFormater = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * @description: 列值转换为字符串
     */
    public static String getStringByColumnValue(CanalEntry.Column column) {
        JDBCType jdbcType = JDBCType.valueOf(column.getSqlType());
        switch (jdbcType) {
            case BIT:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case REAL:
            case DOUBLE:
            case NUMERIC:
            case DECIMAL:
                return column.getValue();
            case CHAR:
            case VARCHAR:
            case LONGVARCHAR:
            case LONGNVARCHAR:
                return with(column.getValue());
//            case TIME:
//                Time time = (Time) itemDataVo.getValue();
//                return with(LocalDateTime.ofInstant(Instant.ofEpochMilli(time.getTime()), UTC).format(DateTimeFormatter.ISO_LOCAL_TIME));
//            case TIMESTAMP:
//                if (ColumnType.DATETIME_V2.equals(itemDataVo.getColumn().getColumnType())) {
//                    return with(formatDateTime((Date) itemDataVo.getValue(), UTC));
//                } else if (ColumnType.TIMESTAMP_V2.equals(itemDataVo.getColumn().getColumnType())) {
//                    return with(formatDateTime((Date) itemDataVo.getValue(), ZoneId.systemDefault()));
//                } else {
//                    throw new RuntimeException("不支持的类型 " + itemDataVo.getColumn().getColumnType());
//                }
            case DATE:
                return with(column.getValue());
            case BLOB:
            case LONGVARBINARY:
                return "0x" + DatatypeConverter.printHexBinary(column.getValue().getBytes());
            default:
                throw new RuntimeException("不能识别的类型 " + column);
        }
    }

    private static String formatDateTime(Date date, ZoneId zoneOffset) {
        Instant instant = Instant.ofEpochMilli(date.getTime());
        return dateTimeFormater.format(LocalDateTime.ofInstant(instant, zoneOffset));
    }

    public static String with(String value) {
        return "'" + value + "'";
    }

}
