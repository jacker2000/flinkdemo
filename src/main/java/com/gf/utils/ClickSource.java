package com.gf.utils;


import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 *  todo
 *   目的:产生数据流
 *   SourceFunction:必须指定泛型,可以实现非并行源
 *   ParallelSourceFunction:必须指定泛型,可以实现并行源
 *   RichParallelSourceFunction 并行源来编写自己的自定义源
 *   事件驱动
 */
public class ClickSource implements SourceFunction<ClickEvent> {
    private boolean running =true;//标志位
    private Random random =new Random();
    private String[] userArray={"Mary", "Bob", "Alice", "John", "Rose", "Liz"};
    private String[] urlArray={"./home", "./cart", "./buy", "./fav", "./prod?id=1", "./prod?id=2"};

    //当程序启动时，触发run方法调用

    /**
     *  todo
     *      run方法的参数SourceContext是上下文，当实例化信息
     * @param cs
     * @throws Exception
     */
    @Override
    public void run(SourceContext<ClickEvent> cs) throws Exception {
        while (running){
            //向下游发送数据
            cs.collect(new ClickEvent(
                    userArray[random.nextInt(userArray.length)],
                    urlArray[random.nextInt(urlArray.length)],
                    Calendar.getInstance().getTimeInMillis() //事件发生时间
            ));
            //为了不让数据发送太快，1000毫秒,1秒
            Thread.sleep(1000L);
        }
    }

    //当取消作业时，触发cancel方法调用
    //命令行:flink cancel JobId
    @Override
    public void cancel() {
        running=false;
    }
}
