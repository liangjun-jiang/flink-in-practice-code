package com.flinkinpractice.chapter4.Watermarks;

import com.flinkinpractice.chapter4.function.WordPunctuatedWatermark;
import com.flinkinpractice.chapter4.models.Word;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *
 * lion,1,1571174849507
 * lion,2,1571174849509
 * lion,3,1571174849601
 *
 */

public class WatermarkPunctuatedExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        SingleOutputStreamOperator<Word> data = env.socketTextStream("localhost", 9000)
                .map(new MapFunction<String, Word>() {
                    @Override
                    public Word map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new Word(split[0], Integer.valueOf(split[1]), Long.valueOf(split[2]));
                    }
                });

        // Punctuated Watermark
        data.assignTimestampsAndWatermarks(new WordPunctuatedWatermark());

        data.print();
        env.execute("Punctuated watermark demo");
    }
}
