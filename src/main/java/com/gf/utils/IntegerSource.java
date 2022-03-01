package com.gf.utils;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 *  todo
 *      产生整型数据源
 *
 */
public class IntegerSource implements SourceFunction<Integer> {
    private boolean running =true;
    private Random  random=new Random();

    @Override
    public void run(SourceContext<Integer> sc) throws Exception {
        //如果不想无限产生数据，只产生一条数据，则sc.collect(random.nextInt(1000));

        while (running){
               sc.collect(random.nextInt(1000));
               Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running=false;
    }
}
