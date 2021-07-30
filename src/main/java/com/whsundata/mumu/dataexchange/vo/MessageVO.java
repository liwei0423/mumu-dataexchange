package com.whsundata.mumu.dataexchange.vo;

import com.alibaba.otter.canal.protocol.CanalEntry;
import lombok.Data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @description: 解析消息封装类
 * @author: liwei
 * @date: 2021/7/29
 */
@Data
public class MessageVO implements Serializable {

    /**
     * @description: 主键信息
     */
    private Map<String, String> primarykeyMap = new HashMap<>();
    /**
     * @description: 行数据信息
     */
    private Map<String, CanalEntry.Column> rowDataMap = new LinkedHashMap<>();

}
