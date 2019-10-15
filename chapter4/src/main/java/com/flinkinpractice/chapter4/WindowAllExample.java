package com.flinkinpractice.chapter4;

import com.flinkinpractice.chapter4.function.LineSplitter;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 *
 * windowAll() is used for non-keyed streams and required "assigner"
 * To run this program,
 * 1. run `nc -l 9000` in your terminal window
 * 2. type numbers line by line, such as
 * 1 2 3
 * 4 5 6
 */
public class WindowAllExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 9000);

        dataStream.flatMap(new LineSplitter())
                .keyBy(0)
                .timeWindowAll(Time.seconds(10));

        env.execute("flink windowAll example");
    }
}
