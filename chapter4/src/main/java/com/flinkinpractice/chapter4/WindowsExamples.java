package com.flinkinpractice.chapter4;

import com.flinkinpractice.chapter4.function.ExecutionEnvUtil;
import com.flinkinpractice.chapter4.function.LineSplitter;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import static com.flinkinpractice.chapter4.constant.WindowConstants.HOST_NAME;
import static com.flinkinpractice.chapter4.constant.WindowConstants.PORT;

public class WindowsExamples {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> data = env.socketTextStream("localhost", 9000);

        // 基于时间窗口
/*        data.flatMap(new LineSplitter())
                .keyBy(1)
                .timeWindow(Time.seconds(30))
                .sum(0)
                .print();*/

        //基于滑动时间窗口
/*        data.flatMap(new LineSplitter())
                .keyBy(1)
                .timeWindow(Time.seconds(60), Time.seconds(30))
                .sum(0)
                .print();*/


        //基于事件数量窗口
/*        data.flatMap(new LineSplitter())
                .keyBy(1)
                .countWindow(3)
                .sum(0)
                .print();*/


        //  基于事件数量滑动窗口
/*        data.flatMap(new LineSplitter())
                .keyBy(1)
                .countWindow(4, 3)
                .sum(0)
                .print();*/


        // 基于会话时间窗口
        data.flatMap(new LineSplitter())
                .keyBy(1)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5))) //表示如果 5s 内没出现数据则认为超出会话时长，然后计算这个窗口的和
                .sum(0)
                .print();

        env.execute("Window and watermark in Flink");
    }
}
