package com.flinkinpractice.chapter4.Windows;

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
 * 3 lions
 * 4 tigers
 * ... ...
 */

// Non-Keyed and Processing Time based windowAll Window
public class NonKeyedTumblingWindowExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 9000);

        dataStream.flatMap(new LineSplitter())
                .timeWindowAll(Time.seconds(10))
                .sum(0)
                .print();

        env.execute("Non-keyed tumbling window example");
    }
}
