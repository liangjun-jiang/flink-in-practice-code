package com.flinkinpractice.chapter4.Windows;

import com.flinkinpractice.chapter4.function.CustomSource;
import com.flinkinpractice.chapter4.function.ExecutionEnvUtil;
import com.flinkinpractice.chapter4.models.WordEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;

@Slf4j
public class KeyedEventTimeBasedTumblingWindowExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<WordEvent> data = env.addSource(new CustomSource())
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<WordEvent>() {
                    private long currentTimestamp = Long.MIN_VALUE;

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1);
                    }

                    @Override
                    public long extractTimestamp(WordEvent element, long previousElementTimestamp) {
                        if (element.getTimestamp() > currentTimestamp) {
                            this.currentTimestamp = element.getTimestamp();
                        }
                        return currentTimestamp;
                    }
                });

        data.keyBy(WordEvent::getWord)
                .timeWindow(Time.seconds(5))
                .sum("count")
                .print();

        env.execute("keyed and event based tumbling window");
    }
}
