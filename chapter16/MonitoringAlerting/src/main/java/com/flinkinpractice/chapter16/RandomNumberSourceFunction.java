package com.flinkinpractice.chapter16;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.ThreadLocalRandom;

public class RandomNumberSourceFunction implements SourceFunction<Integer> {
    private int count = 0;
    private volatile boolean isRunning = true;
    private int elements;

    RandomNumberSourceFunction(int elements) {
        this.elements = elements;
    }

    public void run(SourceFunction.SourceContext<Integer> ctx) throws InterruptedException {
        while (isRunning && count < elements) {
            Thread.sleep(1);
            ctx.collect(ThreadLocalRandom.current().nextInt(10_000));
            count++;
        }
    }

    public void cancel() {
        isRunning = false;
    }
}
