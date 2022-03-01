package com.gf.utils;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class SensorSource implements SourceFunction<SensorReading> {
    private Random random =new Random();
    private boolean running =true;

    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {
        while (running){
            for (int i = 1; i < 4; i++) {
                ctx.collect(new SensorReading("sensor_"+i,random.nextInt()));
            }
        }
        Thread.sleep(300L);
    }

    @Override
    public void cancel() {
        running=false;
    }
}
