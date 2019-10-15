package com.flinkinpractice.chapter4.Watermarks;

import com.flinkinpractice.chapter4.function.WordPeriodicWatermark;
import com.flinkinpractice.chapter4.models.Word;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

public class WatermarkSideOutputLateDataExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        OutputTag<Word> lateDataTag = new OutputTag<Word>("late") {
        };

        SingleOutputStreamOperator<Word> data = env.socketTextStream("localhost", 9001)
                .map(new MapFunction<String, Word>() {
                    @Override
                    public Word map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new Word(split[0], Integer.valueOf(split[1]), Long.valueOf(split[2]));
                    }
                }).assignTimestampsAndWatermarks(new WordPeriodicWatermark());

        SingleOutputStreamOperator<Word> sum = data.keyBy(0)
                .timeWindow(Time.seconds(10))
                .sideOutputLateData(lateDataTag)
                .sum(1);

        sum.print();

        sum.getSideOutput(lateDataTag)
                .print();

        env.execute("Side output late data watermark demo");
    }

}
