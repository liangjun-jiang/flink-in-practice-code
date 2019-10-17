package com.flinkinpractice.chapter7;

import com.flinkinpractice.chapter7.DataSource.OrderEventSource;
import org.apache.flink.api.java.tuple.Tuple3;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.*;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;

import java.util.List;
import java.util.Map;


/**
 *
 * https://blog.csdn.net/lvwenyuan_1/article/details/93080820
 */
public class CEPOrderExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<String, String, String>> myDataStream = env.addSource(new OrderEventSource()).map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                JSONObject json = JSON.parseObject(value);
                return new Tuple3<>(json.getString("userid"), json.getString("orderid"), json.getString("behave"));
            }
        });

        /**
         * 定义一个规则
         * 接受到behave是order以后，下一个动作必须是pay才算符合这个需求
         */
        Pattern<Tuple3<String, String, String>, Tuple3<String, String, String>> myPattern = Pattern.<Tuple3<String, String, String>>begin("start").where(new IterativeCondition<Tuple3<String, String, String>>() {
            @Override
            public boolean filter(Tuple3<String, String, String> value, Context<Tuple3<String, String, String>> context) throws Exception {
//                System.out.println("value:" + value);
                return value.f2.equals("order");
            }
        }).next("next").where(new IterativeCondition<Tuple3<String, String, String>>() {
            @Override
            public boolean filter(Tuple3<String, String, String> value, Context<Tuple3<String, String, String>> ctx) throws Exception {
                return value.f2.equals("pay");
            }
        }).within(Time.seconds(3));


        PatternStream<Tuple3<String, String, String>> patternStream = CEP.pattern(myDataStream.keyBy(0), myPattern);
        DataStream<String> validTranStream = patternStream.flatSelect(new PatternFlatSelectFunction<Tuple3<String, String, String>, String>() {
            @Override
            public void flatSelect(Map<String, List<Tuple3<String, String, String>>> pattern, Collector<String> collector) throws Exception {
//                Tuple3<String, String, String> order = pattern.get("start");

            }
        });
        validTranStream.print();

        DataStream<String> result = patternStream.process(new PatternProcessFunction<Tuple3<String, String, String>, String>() {
            @Override
            public void processMatch(Map<String, List<Tuple3<String, String, String>>> map, Context context, Collector<String> collector) throws Exception {

            }
        });


        //记录超时的订单
        OutputTag<String> outputTag = new OutputTag<String>("myOutput") {
        };

        SingleOutputStreamOperator<String> resultStream = patternStream.select(outputTag,
                /**
                 * 超时的
                 */
                new PatternTimeoutFunction<Tuple3<String, String, String>, String>() {
                    @Override
                    public String timeout(Map<String, List<Tuple3<String, String, String>>> pattern, long timeoutTimestamp) throws Exception {
//                        System.out.println("pattern:" + pattern);
                        List<Tuple3<String, String, String>> startList = pattern.get("start");
                        Tuple3<String, String, String> tuple3 = startList.get(0);
                        return tuple3.toString() + "迟到的";
                    }
                }, new PatternSelectFunction<Tuple3<String, String, String>, String>() {
                    @Override
                    public String select(Map<String, List<Tuple3<String, String, String>>> pattern) throws Exception {
                        //匹配上第一个条件的
                        List<Tuple3<String, String, String>> startList = pattern.get("start");
                        //匹配上第二个条件的
                        List<Tuple3<String, String, String>> endList = pattern.get("next");

                        Tuple3<String, String, String> tuple3 = endList.get(0);
                        return tuple3.toString();
                    }
                }
        );

        //输出匹配上规则的数据
        resultStream.print();

        //输出超时数据的流
//        DataStream<String> sideOutput = resultStream.getSideOutput(outputTag);
//        sideOutput.print();

        env.execute("Test CEP");
    }
}
