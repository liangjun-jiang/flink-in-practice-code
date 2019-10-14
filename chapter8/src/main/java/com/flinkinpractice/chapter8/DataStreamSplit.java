package com.flinkinpractice.chapter8;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class DataStreamSplit {
    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        final long rate = params.getLong("rate", 3L);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple2<String, String>> alertsStream = AlertSource.getSource(env, rate);
//        alertsStream.print();

        SplitStream<Tuple2<String, String>> splitStream = alertsStream.split(new OutputSelector<Tuple2<String, String>>() {
            @Override
            public Iterable<String> select(Tuple2<String, String> alertTuple) {
                List<String> tags = new ArrayList<>();
                switch (alertTuple.f0) {
                    case "DOCKER":
                        tags.add("DOCKER");
                        break;
                    case "MIDDLE_WARE":
                        tags.add("MIDDLE_WARE");
                        break;
                    case "HEALTH_CHECK":
                        tags.add("HEALTH_CHECK");
                        break;
                }

                return tags;
            }
        });

        DataStream<Tuple2<String, String>> middlewareStream = splitStream.select("MIDDLE_WARE");
//        middlewareStream.print();

        middlewareStream.filter(new FilterFunction<Tuple2<String, String>>() {
            @Override
            public boolean filter(Tuple2<String, String> alertTuple) throws Exception {
                return alertTuple.f1.equalsIgnoreCase("RECOVERING");
            }
        }).print();

        env.execute("Split and filter with Flink");
    }
}