package com.flinkinpratice.chapter11.utils;

import com.alibaba.fastjson.JSON;
import com.flinkinpratice.chapter11.model.Student;
import lombok.extern.slf4j.XSlf4j;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;

@XSlf4j
public class KafkaProducerUtil {
    public static final String brokerServers = "localhost:9092";

    public static final String topic = "student";

    public static void main(String[] args) {
        sendMessage();
    }

    private static void sendMessage() {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerServers);
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        prop.put(ProducerConfig.ACKS_CONFIG, "all");
        prop.put(ProducerConfig.BATCH_SIZE_CONFIG, "10000");
        prop.put(ProducerConfig.RETRIES_CONFIG, 0);

        KafkaProducer producer = new KafkaProducer(prop);

        //循环发送
        for (int i = 188; i <= 198; i++) {
            Student student = new Student(i, "stu_" + i, "123456", 10 + i);
            ProducerRecord<String,String> record = new ProducerRecord<>(topic, JSON.toJSONString(student));
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) exception.printStackTrace();
                    String data = "message send to => topic: " + metadata.topic() + ",partition: " + metadata.partition() + ",offset: " + metadata.offset()+",value: "+record.value();
                    System.err.println(data);
                }
            });
        }
        producer.flush();
        producer.close();
    }
}
