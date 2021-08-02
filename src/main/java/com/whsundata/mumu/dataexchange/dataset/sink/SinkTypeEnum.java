package com.whsundata.mumu.dataexchange.dataset.sink;

/**
 * @description: 操作类型enum
 * @author: liwei
 * @date: 2021/8/2
 */
public enum SinkTypeEnum {

    /**
     * 增删改
     */
    CREATE("c", "新增数据"),
    UPDATE("u", "更新数据"),
    DELETE("d", "删除数据");


    private String code;

    private String desc;

    SinkTypeEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public String getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }
}
