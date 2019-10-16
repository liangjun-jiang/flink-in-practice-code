package com.flinkinpratice.chapter11;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * consuming Kafka message and use MySQL as the sink，implemented the two-phase-commit to make sure the end-to-end exactly once
 */

@SuppressWarnings("all")
public class Kafka2Kafka2MysqlExactOnceExample {
    private static final String topic_ExactlyOnce = "mysql-exactly-once";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // checkpoint is set to 10 seconds
        env.enableCheckpointing(10000);
        // use: exactly_one
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // set the min. time between two checkpoints
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        // set the timeout time of a checkpoint. data will be discarded if > 10s
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        // set the concurrent checkpoints
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // checkpoint data will be kept once Flink appp is canceled
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // set where to keep statebackend. Here we keep it locally
//        env.setStateBackend(new FsStateBackend("file:///Users/temp/cp/"));

        // Consume Kafka messages
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "flink-consumer-group2");
        props.put(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, "3000");

        // Use SimpleStringSchema to obtain Kafka message, and deserialized the message details such as key,value，metadata:topic,partition，offset etc
        FlinkKafkaConsumer<ObjectNode> kafkaConsumer = new FlinkKafkaConsumer<>(topic_ExactlyOnce, new JSONKeyValueDeserializationSchema(true), props);

        // kafka consumer as source
        DataStreamSource<ObjectNode> streamSource = env.addSource(kafkaConsumer);

        streamSource.print();

        // sending to the sinker directly
        streamSource.addSink(new MySqlTwoPhaseCommitSink()).name("MySqlTwoPhaseCommitSink");

        env.execute(Kafka2Kafka2MysqlExactOnceExample.class.getName());
    }
}

