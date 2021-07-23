package com.whsundata.mumu.dataexchange.kafka;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

@Component("kafkaExecutor")
public class KafkaExecutor {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private KafkaProducer<String, String> producer;

    private KafkaConsumer<String, String> consumer;

    public void createTopic(String topic) {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "10.0.40.217:9092");

        AdminClient admin = AdminClient.create(prop);

        List<NewTopic> topics = new ArrayList<NewTopic>();
        // 创建主题  参数：主题名称、分区数、副本数
        NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
        topics.add(newTopic);

        CreateTopicsResult result = admin.createTopics(topics);
        try {
            result.all().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public void deleteTopic(String topic) {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "10.0.40.217:9092");
        AdminClient client = AdminClient.create(prop);

        ArrayList<String> topics = new ArrayList<>();
        topics.add(topic);

        DeleteTopicsResult result = client.deleteTopics(topics);

        try {
            result.all().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public void listTopics() {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "10.0.40.217:9092");
        AdminClient admin = AdminClient.create(prop);

        ListTopicsResult result = admin.listTopics();
        KafkaFuture<Set<String>> future = result.names();

        try {
            System.out.println("==================Kafka Topics====================");
            future.get().forEach(n -> System.out.println(n));
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public void sendMessage(String topic, String msg) {
//        kafkaTemplate.send(topic, msg);
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.0.40.217:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> producerRecord = new ProducerRecord(topic,"user", msg);
        producer.send(producerRecord);
        producer.flush();
    }

    public void listen(String groupId, String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.0.40.217:9092");
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(200));
        consumer.close();
        for (ConsumerRecord record : records) {
            Iterator<Header> iterator = record.headers().iterator();
            System.out.println("------ header -------");
            while (iterator.hasNext()) {
                Header header = iterator.next();
                System.out.println(header.key() + "---->" + header.value());
            }
            System.out.println("record key=" + record.key());
            System.out.println("record value=" + record.value());
        }
    }

    public static void main(String[] args) {
        KafkaExecutor kafkaExecutor = new KafkaExecutor();
        String topic = "dev-test-user";
//        kafkaExecutor.createTopic(topic);
//        kafkaExecutor.sendMessage(topic, "abc123");
        kafkaExecutor.listen("0", topic);
    }
}
