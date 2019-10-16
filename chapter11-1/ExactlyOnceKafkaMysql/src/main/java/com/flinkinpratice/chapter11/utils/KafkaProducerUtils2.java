package com.flinkinpratice.chapter11.utils;

import com.alibaba.fastjson.JSON;
import com.flinkinpratice.chapter11.model.MysqlExactlyOncePOJO;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerUtils2 {
    private static final String broker_list = "localhost:9092";
    private static final String topic_ExactlyOnce = "mysql-exactly-once";

    public static void writeToKafka2(Producer<String, String> producer, int i) throws InterruptedException {
        MysqlExactlyOncePOJO mysqlExactlyOnce = new MysqlExactlyOncePOJO(String.valueOf(i));
        ProducerRecord record = new ProducerRecord<String, String>(topic_ExactlyOnce, null, null, JSON.toJSONString(mysqlExactlyOnce));
        producer.send(record);
        System.out.println("Publishing message: " + JSON.toJSONString(mysqlExactlyOnce));
    }

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
        try {
            for (int i = 21; i <= 40; i++) {
                writeToKafka2(producer, i);
                Thread.sleep(1000);
            }
        }catch (Exception e){
            throw e;
        }
        producer.flush();
    }
}


