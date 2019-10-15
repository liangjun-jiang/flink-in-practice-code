package com.flinkinpractice.chapter4.function;

import com.flinkinpractice.chapter4.models.WordEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

@Slf4j
public class CustomTrigger extends Trigger<WordEvent, TimeWindow> {

    ReducingState<Long> stateDesc;

    private long interval;

    private CustomTrigger(long interval) {
        this.interval = interval;
    }


    public CustomTrigger() {
        super();
    }

    //每个元素被添加到窗口时都会调用该方法
    @Override
    public TriggerResult onElement(WordEvent element, long timestamp, TimeWindow window, Trigger.TriggerContext ctx) throws Exception {
        log.info("======onElement====window start = {}, window end = {}", window.getStart(), window.getEnd());

        return TriggerResult.CONTINUE;
//        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
//
//        timestamp = ctx.getCurrentProcessingTime();
//
//        if (fireTimestamp.get() == null) {
//            long start = timestamp - (timestamp % interval);
//            long nextFireTimestamp = start + interval;
//
//            ctx.registerProcessingTimeTimer(nextFireTimestamp);
//
//            fireTimestamp.add(nextFireTimestamp);
//            return TriggerResult.CONTINUE;
//        }
//        return TriggerResult.CONTINUE;

    }

    //当一个已注册的 ProcessingTime 计时器启动时调用
    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        System.out.println("======onProcessingTime====");

        return null;
//        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
//
//        if (fireTimestamp.get().equals(time)) {
//            fireTimestamp.clear();
//            fireTimestamp.add(time + interval);
//            ctx.registerProcessingTimeTimer(time + interval);
//            return TriggerResult.FIRE;
//        } else if(window.maxTimestamp() == time) {
//            return TriggerResult.FIRE;
//        }
//        return TriggerResult.CONTINUE;
    }

    //当一个已注册的 EventTime 计时器启动时调用
    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        System.out.println("======onEventTime====");

        return null;
//        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
//
//        if (fireTimestamp.get().equals(time)) {
//            fireTimestamp.clear();
//            fireTimestamp.add(time + interval);
//            ctx.registerProcessingTimeTimer(time + interval);
//            return TriggerResult.FIRE;
//        } else if(window.maxTimestamp() == time) {
//            return TriggerResult.FIRE;
//        }
//        return TriggerResult.CONTINUE;
    }

    //与状态性触发器相关，当使用会话窗口时，两个触发器对应的窗口合并时，合并两个触发器的状态
    @Override
    public void onMerge(TimeWindow window, OnMergeContext ctx) throws Exception {
        super.onMerge(window, ctx);
    }

    //执行任何需要清除的相应窗口
    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
    }

    public static CustomTrigger creat() {
        return new CustomTrigger();
    }
}