package com.whsundata.mumu.dataexchange.vo;

import com.alibaba.otter.canal.protocol.CanalEntry;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @description: 
 * @author: liwei
 * @date: 2021/7/29
 */
@Data
public class MessageVO implements Serializable {

    private Map<String, String> primarykeyMap = new HashMap<>();

    private List<CanalEntry.Column> RowDataList = new ArrayList<>();
}
