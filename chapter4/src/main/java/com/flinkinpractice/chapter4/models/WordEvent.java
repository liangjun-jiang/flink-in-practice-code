package com.flinkinpractice.chapter4.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class WordEvent {
    private String word;
    private int count;
    private long timestamp;
}
