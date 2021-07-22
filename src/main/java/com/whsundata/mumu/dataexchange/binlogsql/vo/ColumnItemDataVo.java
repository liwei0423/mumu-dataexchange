package com.whsundata.mumu.dataexchange.binlogsql.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class ColumnItemDataVo {
    private Object value;
    private ColumnVo column;
}
