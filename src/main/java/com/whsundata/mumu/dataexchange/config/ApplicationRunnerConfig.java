package com.whsundata.mumu.dataexchange.config;

import com.whsundata.mumu.dataexchange.binlogsql.Bootstrap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * @description: 应用启动后执行
 * @author: liwei
 * @create: 2021/6/1 9:55
 **/
@Slf4j
@Component
public class ApplicationRunnerConfig implements ApplicationRunner {

    @Override
    public void run(ApplicationArguments args) {
        Bootstrap.start();
        log.info("启动初始化完成");
    }
}
