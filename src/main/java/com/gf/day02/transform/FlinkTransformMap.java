package com.gf.day02.transform;

import com.gf.utils.ClickEvent;
import com.gf.utils.ClickSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *  Map方式实现
 */
public class FlinkTransformMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new ClickSource()) //数据源导入
                /**
                 *  todo
                 *      MapFunction
                 *          第一个参数：输入的泛型，
                 *          第二个参数：输出的泛型
                 */
                .map(new MapFunction<ClickEvent, String>() {
                    @Override
                    public String map(ClickEvent clickEvent) throws Exception {
                        //输出数据
                        return clickEvent.username;
                    }
                })
                .print("匿名类:");
        env
                .addSource(new ClickSource())
                .map(new MyMap())
                .print("外部类:");

        env
                .addSource(new ClickSource())
                .map(r->r.username)
                .print("匿名函数:");
        /**
         *  todo
         *      flatMap是更底层的算子
         */
        env
                .addSource(new ClickSource())
                .flatMap(new FlatMapFunction<ClickEvent, String>() {
                    /**
                     *  输入是ClickEvent，输出是字符串
                     * @param clickEvent
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void flatMap(ClickEvent clickEvent, Collector<String> out) throws Exception {
                        out.collect(clickEvent.username);
                    }
                })
                .print("使用flatMap方式实现:");


        env.execute();

    }
   public static  class MyMap implements MapFunction<ClickEvent,String>{

       @Override
       public String map(ClickEvent clickEvent) throws Exception {
           return clickEvent.username;
       }
   }
}
