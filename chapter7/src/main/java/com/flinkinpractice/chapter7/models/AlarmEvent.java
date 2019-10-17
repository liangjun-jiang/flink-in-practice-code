package com.flinkinpractice.chapter7.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class AlarmEvent {
    int eventNumber;
    String resourceId;
    Severity severity;
    int probableCause;
    String specificProblem;
    long eventTime;

    public AlarmEvent(int eventNumber, String resourceId, Severity severity) {
        this.eventNumber = eventNumber;
        this.resourceId = resourceId;
        this.severity = severity;
        eventTime = System.currentTimeMillis();
    }

    public Severity getSeverity() {
        return  this.severity;
    }
}
