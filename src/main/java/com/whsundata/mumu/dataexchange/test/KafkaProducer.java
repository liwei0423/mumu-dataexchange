package com.whsundata.mumu.dataexchange.test;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @description:
 * @author: liwei
 * @date: 2021/8/2
 */
public class KafkaProducer {

    public static void send(String topic, String key, String value) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.0.40.217:9092");
        props.put("batch.size", "16384");
        props.put("buffer.memory", "33554432");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);

        ProducerRecord<String, String> producerRecord = new ProducerRecord(topic, key, value);
        producer.send(producerRecord);
        producer.flush();
    }
}
