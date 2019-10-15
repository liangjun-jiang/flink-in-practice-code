package com.flinkinpractice.chapter4.Watermarks;

import com.flinkinpractice.chapter4.models.Word;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WatermarkBoundedOutOfOrdernessExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        SingleOutputStreamOperator<Word> data = env.socketTextStream("localhost", 9001)
                .map(new MapFunction<String, Word>() {
                    @Override
                    public Word map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new Word(split[0], Integer.valueOf(split[1]), Long.valueOf(split[2]));
                    }
                });

        //BoundedOutOfOrdernessTimestampExtractor
        data.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Word>(Time.seconds(10)) {
            @Override
            public long extractTimestamp(Word element) {
                return element.getTimestamp();
            }
        });

        data.print();
        env.execute("Bounded OutOf Orderness watermark demo");
    }
}
