package com.whsundata.mumu.dataexchange.test;

import com.whsundata.mumu.dataexchange.DataApplication;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Properties;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = DataApplication.class)//这里的Application是springboot的启动类名
public class KafkaTest {

    @Autowired
    KafkaTemplate kafkaTemplate;

    @Autowired
    ConsumerFactory consumerFactory;

    @Test
    public void send(){
        String topic = "dev-test-user";
        kafkaTemplate.send(topic,"testtemplate");
    }

//    @Test
//    public void listen(){
//        String topic = "dev-test-user";
//        ContainerProperties properties = new ContainerProperties(topic);
//        properties.setMessageListener(new KafkaConsumer());
//        new KafkaMessageListenerContainer<>(consumerFactory, properties);
//    }

    public static void send(String topic, String key, String value) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.0.40.217:9092");
        props.put("batch.size", "16384");
        props.put("buffer.memory", "33554432");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        ProducerRecord<String, String> producerRecord = new ProducerRecord(topic, key, value);
        producer.send(producerRecord);
        producer.flush();
    }

    public static void main(String[] args) {
        String topic = "dev-test-user";
        send(topic, "user", "kkkkk");
    }
}
