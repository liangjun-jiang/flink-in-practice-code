package com.flinkinpractice.chapter4;

import com.flinkinpractice.chapter4.function.CustomSource;
import com.flinkinpractice.chapter4.function.ExecutionEnvUtil;
import com.flinkinpractice.chapter4.models.WordEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;

@Slf4j
public class WatermarkExample1 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //如果不指定时间的话，默认是 ProcessingTime，但是如果指定为事件事件的话，需要事件中带有时间或者添加时间水印
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;
        DataStream<WordEvent> data = env.addSource(new CustomSource())
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<WordEvent>() {
                    private long currentTimestamp = Long.MIN_VALUE;

                    private final long maxTimeLag = 5000;

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

        env.execute("zhisheng —— flink window example");
    }
}
