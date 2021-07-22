package com.whsundata.mumu.dataexchange.binlogsql.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class FilterDbTableVo {
    private String dbName;
    private String tableName;
}
