package com.flinkinpractice.chapter11;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

@Slf4j
public class KafkaUtils {
    private static final String broker_list = "localhost:9092";
    private static final String topic = "student-1";  // kafka topic needs to match the topic defined in the Main.java

    public static void writeToKafka() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);

        try {
            for (int i = 1; i <= 100; i++) {
                Student student = new Student(i, "flinkinpractice" + i, "password" + i, 18 + i);
                ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, GsonUtil.toJson(student));
                producer.send(record);
                log.debug("publish  message: " + GsonUtil.toJson(student));
            }
            Thread.sleep(3000);
        }catch (Exception e){

        }

        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        writeToKafka();
    }
}
