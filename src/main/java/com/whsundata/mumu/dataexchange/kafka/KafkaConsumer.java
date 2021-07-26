package com.whsundata.mumu.dataexchange.kafka;

import com.whsundata.mumu.dataexchange.util.HttpClientUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class KafkaConsumer {

    @KafkaListener(topics = "dev-test-user")
    public void onMessage(ConsumerRecord<String, String> data) {
        String value = data.value();
        System.out.println("consumer value=" + value);
        try {
            Map<String, Object> params = new HashMap<>();
            params.put("sql", value);
            String result = HttpClientUtil.httpPostRequest("http://localhost:8080/demo/receive/", params);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

