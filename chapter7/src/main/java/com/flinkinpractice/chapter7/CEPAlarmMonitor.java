package com.flinkinpractice.chapter7;

import com.flinkinpractice.chapter7.DataSource.AlarmEventSource;
import com.flinkinpractice.chapter7.models.AlarmEvent;
import com.flinkinpractice.chapter7.models.Severity;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.types.Either;
import org.apache.flink.streaming.api.windowing.time.Time;


import java.util.List;
import java.util.Map;


/**
 *
 * reference: https://github.com/ravthiru/flink-cep-examples/blob/master/src/main/java/flinkcep/examples/common/Severity.java
 */
public class CEPAlarmMonitor {
    private static final long PAUSE = 5000;
    private static final double TEMP_STD = 20;
    private static final double TEMP_MEAN = 80;

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // setting Parallelism to 1
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Input stream of alarm events, event creation time is take as timestamp
        // Setting the Watermark to same as creation time of the event.
        DataStream<AlarmEvent> inputEventStream = env
                .addSource(new AlarmEventSource(PAUSE, TEMP_STD, TEMP_MEAN))
                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<AlarmEvent>() {

                    @Override
                    public long extractTimestamp(AlarmEvent event, long currentTimestamp) {
                        return event.getEventTime();
                    }

                    @Override
                    public Watermark checkAndGetNextWatermark(AlarmEvent lastElement, long extractedTimestamp) {
                        return new Watermark(extractedTimestamp);
                    }

                });

        //Continuously prints the input events
        inputEventStream.print();

        // Wait for 3 seconds and then decide if the event is really a critical issue
        // in the network element, I have used larger pause time between the event
        // to simulate time-out
        Pattern<AlarmEvent, ?> alarmPattern = Pattern.<AlarmEvent>begin("first")
                .where(new IterativeCondition<AlarmEvent>() {
                    @Override
                    public boolean filter(AlarmEvent alarmEvent, Context context) throws Exception {
                        return alarmEvent.getSeverity().getValue() == Severity.CRITICAL.getValue();
                    }
                })
                .next("second")
                .where(new IterativeCondition<AlarmEvent>() {
                    @Override
                    public boolean filter(AlarmEvent alarmEvent, Context context) throws Exception {
                        return alarmEvent.getSeverity().getValue() == Severity.CLEAR.getValue();
                    }
                })
                .within(Time.seconds(3));



        DataStream<Either<String, String>> result = CEP.pattern(inputEventStream, alarmPattern).
                select(new PatternTimeoutFunction<AlarmEvent, String>() {

                    @Override
                    public String timeout(Map<String, List<AlarmEvent>> pattern, long l) throws Exception {
                        System.out.println("Timeout "+pattern);
                        return pattern.get("first").toString() + "";
                    }
                },new PatternSelectFunction<AlarmEvent, String>() {
                    @Override
                    public String select(Map<String, List<AlarmEvent>> pattern) throws Exception {
                        StringBuilder builder = new StringBuilder();

                        builder.append(pattern.get("first").toString());

                        return builder.toString();
                    }
                });

        env.execute("CEP monitoring job");
    }
}
