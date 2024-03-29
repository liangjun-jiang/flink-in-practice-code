package com.flinkinpractice.chapter7.DataSource;

import com.flinkinpractice.chapter7.models.AlarmEvent;
import com.flinkinpractice.chapter7.models.Severity;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;


/**
 *
 * reference: https://github.com/ravthiru/flink-cep-examples/blob/master/src/main/java/flinkcep/examples/common/Severity.java
 */
public class AlarmEventSource extends RichParallelSourceFunction<AlarmEvent> {
    private static final long serialVersionUID = 3589767994783688247L;

    private boolean running = true;

    private final long pause;
    private final double temperatureStd;
    private final double temperatureMean;
    private Random random;

    public AlarmEventSource(long pause, double temperatureStd, double temperatureMean) {
        this.pause = pause;
        this.temperatureMean = temperatureMean;
        this.temperatureStd = temperatureStd;
    }

    @Override
    public void open(Configuration configuration) {
        random = new Random();
    }

    public void run(SourceContext<AlarmEvent> sourceContext) throws Exception {
        while (running) {
            AlarmEvent event = null;
            Severity alarmSeverity = null;
            double temperature = random.nextGaussian() * temperatureStd + temperatureMean;
            if (temperature > temperatureMean) {
                alarmSeverity = Severity.CRITICAL;
            }else if (temperature > (temperatureMean-10) && temperature < temperatureMean) {
                alarmSeverity = Severity.MAJOR;
            }else if (temperature > (temperatureMean-20) && temperature < temperatureMean-10) {
                alarmSeverity = Severity.MINOR;
            }else if (temperature > (temperatureMean-30) && temperature < temperatureMean-20) {
                alarmSeverity = Severity.WARNING;
            }else if (temperature < (temperatureMean-30))  {
                alarmSeverity = Severity.CLEAR;
            }
            event = new AlarmEvent(123, "NE1", alarmSeverity);
            event.setSpecificProblem("Temperature Reached : "+ temperature);
            sourceContext.collect(event);
            Thread.sleep(pause);
        }
    }

    public void cancel() {
        running = false;
    }

}
