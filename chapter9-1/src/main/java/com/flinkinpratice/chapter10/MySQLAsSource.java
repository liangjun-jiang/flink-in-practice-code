package com.flinkinpratice.chapter10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MySQLAsSource {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new MySQLSource()).print();

        env.execute("MySQL as Flink Source");
    }
}
