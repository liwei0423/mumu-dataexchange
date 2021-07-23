package com.whsundata.mumu.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaTest {
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
