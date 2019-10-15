package com.flinkinpractice.chapter4.Windows;

import com.flinkinpractice.chapter4.function.LineSplitter;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


public class KeyedProcessingTimeBasedWindowsExamples {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> data = env.socketTextStream("localhost", 9000);

        // keyed and processing time based tumbling windows

        data.flatMap(new LineSplitter())
                .keyBy(1)
                .timeWindow(Time.seconds(5))
                .sum(0)
                .print();

        // Keyed and processing time based sliding window
        /*
        data.flatMap(new LineSplitter())
                .keyBy(1)
                .timeWindow(Time.seconds(5), Time.seconds(8))
                .sum(0)
                .print();
        */



        // keyed and count based on tumbling window
        /*
        data.flatMap(new LineSplitter())
                .keyBy(1)
                .countWindow(3)
                .sum(0)
                .print();
        */

        //  keyed and count based on sliding window
        /*
       data.flatMap(new LineSplitter())
                .keyBy(1)
                .countWindow(4, 3)
                .sum(0)
                .print();
        */


        // keyed and processing time based session window
        data.flatMap(new LineSplitter())
                .keyBy(1)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5))) // the session window is set to 5 seconds, the sum will be calculated if no more data in the 5 seconds
                .sum(0)
                .print();

        env.execute("Window and watermark in Flink");
    }
}
