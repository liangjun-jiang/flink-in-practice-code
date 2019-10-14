package com.flinkinpractice.chapter9;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class AsyncIOMain {

    public static void main(String[] args) throws Exception {
        String hostname = "localhost";
        Integer port = 9000;

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取数据
        DataStreamSource<String> socketStream = env.socketTextStream(hostname, port);

        socketStream.print();

        DataStream<Tuple2<String, String>> resultStream =
                AsyncDataStream.unorderedWait(socketStream, new DatabaseAsyncClient(), 1000, TimeUnit.MILLISECONDS, 100);

        resultStream.print();

        env.execute("Async IO  with Flink");
    }





}
