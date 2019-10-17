package com.flinkinpractice.chapter7.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class LoginEvent {
    private String userId;
    private String ip;
    private String type;
}
