package com.flinkinpractice.chapter4.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Word {
    private String word;
    private int count;
    private long timestamp;
}
