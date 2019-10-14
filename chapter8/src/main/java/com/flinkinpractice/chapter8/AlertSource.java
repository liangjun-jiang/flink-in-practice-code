package com.flinkinpractice.chapter8;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;

@SuppressWarnings("serial")
class AlertSource implements Iterator<Tuple2<String, String>>, Serializable {

    static final String[] ALERT_TYPES = {"DOCKER", "MIDDLE_WARE", "HEALTH_CHECK"};
    static final String[] ALERT_STATUS = {"WARNING", "RECOVERING"};

    private final Random rnd = new Random(hashCode());

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public Tuple2<String, String> next() {
        return new Tuple2<>(ALERT_TYPES[rnd.nextInt(ALERT_TYPES.length)], ALERT_STATUS[rnd.nextInt(ALERT_STATUS.length)]);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    public static DataStream<Tuple2<String, String>> getSource(StreamExecutionEnvironment env, long rate) {
        return env.fromCollection(new ThrottledIterator<>(new AlertSource(), rate),
                TypeInformation.of(new TypeHint<Tuple2<String, String>>(){}));
    }
}


