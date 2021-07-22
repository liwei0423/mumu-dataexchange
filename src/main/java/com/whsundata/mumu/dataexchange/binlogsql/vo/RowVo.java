package com.whsundata.mumu.dataexchange.binlogsql.vo;

import lombok.Data;

import java.util.List;


@Data
public class RowVo {
    private List<ColumnItemDataVo> value;
}
