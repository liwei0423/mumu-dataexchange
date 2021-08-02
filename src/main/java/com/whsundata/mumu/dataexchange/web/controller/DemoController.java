package com.whsundata.mumu.dataexchange.web.controller;

import cn.hutool.db.Db;
import cn.hutool.json.JSONUtil;
import com.whsundata.mumu.dataexchange.dataset.sink.SinkMessage;
import com.whsundata.mumu.dataexchange.dataset.sink.SinkTypeEnum;
import com.whsundata.mumu.dataexchange.db.MysqlHelper;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.sql.SQLException;

@RestController
@RequestMapping(value = "/demo")
public class DemoController {

    @RequestMapping(value = "receive")
    public String receive(String sql) {
        System.out.println("--- receive --");
        System.out.println("sql=" + sql);
        System.out.println("----");
        try {
            String sql2 = sql.replace(".`user`",".`user2`");
            Db.use().execute(sql2, null);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return "test success";
    }

    @RequestMapping(value = "receive2")
    public String receive2(String data) {
        System.out.println("--- receive --");
        System.out.println("data=" + data);
        System.out.println("----");
        SinkMessage sinkMessage = JSONUtil.toBean(data, SinkMessage.class);
        try {
            if(SinkTypeEnum.CREATE.getCode().equals(sinkMessage.getSinkType())){
                MysqlHelper.batchPrepareInsert(sinkMessage);
            }else if(SinkTypeEnum.UPDATE.getCode().equals(sinkMessage.getSinkType())){
                MysqlHelper.batchPrepareUpdate(sinkMessage);
            }else if(SinkTypeEnum.DELETE.getCode().equals(sinkMessage.getSinkType())){
                MysqlHelper.batchPrepareDelete(sinkMessage);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return "test success";
    }
}
