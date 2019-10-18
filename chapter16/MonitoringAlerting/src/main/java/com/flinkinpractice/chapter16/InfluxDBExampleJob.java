package com.flinkinpractice.chapter16;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

public class InfluxDBExampleJob {

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(500);
        env.disableOperatorChaining();

        env.addSource(new RandomNumberSourceFunction(parameters.getInt("elements", Integer.MAX_VALUE)))
                .name(RandomNumberSourceFunction.class.getSimpleName())
                .map(new FlinkMetricsExposingMapFunction())
                .name(FlinkMetricsExposingMapFunction.class.getSimpleName())
                .addSink(new DiscardingSink<>())
                .name(DiscardingSink.class.getSimpleName());

        env.execute(InfluxDBExampleJob.class.getSimpleName());
    }
}
