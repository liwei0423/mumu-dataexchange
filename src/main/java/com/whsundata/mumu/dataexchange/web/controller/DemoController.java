package com.whsundata.mumu.dataexchange.web.controller;

import cn.hutool.db.Db;
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
}
