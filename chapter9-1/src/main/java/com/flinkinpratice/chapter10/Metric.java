package com.flinkinpratice.chapter10;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class Metric {
    private String name;
    private long timestamp;
    private Map<String, Object> fields;
    private Map<String, String> tags;
}
