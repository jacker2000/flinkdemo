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
                //高斯函数 中性曲线
                /**
                 *  白噪声
                 *  随机程度比较高，信息伤越大
                 */
                ctx.collect(new SensorReading("sensor_"+i,random.nextGaussian()));
            }
            Thread.sleep(300L);
        }
    }

    @Override
    public void cancel() {
        running=false;
    }
}
