package com.flinkinpractice.chapter4.function;

import com.flinkinpractice.chapter4.models.Word;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

import static com.flinkinpractice.chapter4.function.DateUtil.YYYY_MM_DD_HH_MM_SS;

@Slf4j
public class WordPeriodicWatermark implements AssignerWithPeriodicWatermarks<Word> {

    private long currentTimestamp = Long.MIN_VALUE;

    @Override
    public long extractTimestamp(Word word, long previousElementTimestamp) {
        this.currentTimestamp = word.getTimestamp();
        log.info("event timestamp = {}, {}, CurrentWatermark = {}, {}", word.getTimestamp(),
                DateUtil.format(word.getTimestamp(), YYYY_MM_DD_HH_MM_SS),
                getCurrentWatermark().getTimestamp(),
                DateUtil.format(getCurrentWatermark().getTimestamp(), YYYY_MM_DD_HH_MM_SS));
        return word.getTimestamp();
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        long maxTimeLag = 5000;
        return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - maxTimeLag);
    }
}