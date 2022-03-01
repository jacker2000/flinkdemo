package com.gf.day02.transform;

import com.gf.utils.ClickEvent;
import com.gf.utils.ClickSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *  Filter实现
 */
public class FlinkTransformFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new ClickSource())
                .filter(new FilterFunction<ClickEvent>() {
                    @Override
                    public boolean filter(ClickEvent clickEvent) throws Exception {
                        return clickEvent.username.equals("Mary");
                    }
                })
                .print("匿名类实现filter");

        env
                .addSource(new ClickSource())
                .filter(new MyFilter())
                .print("外部类方式实现filter");

        env
                .addSource(new ClickSource())
                .filter(r->r.username.equals("Mary"))
                .print("匿名函数方式实现filter:");

        env
                .addSource(new ClickSource())
                .flatMap(new FlatMapFunction<ClickEvent, ClickEvent>() {
                    /**
                     *  flatMap，满足条件则原封不动输出，所以两个
                     * @param clickEvent
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void flatMap(ClickEvent clickEvent, Collector<ClickEvent> out) throws Exception {
                        if (clickEvent.username.equals("Mary")) {
                            out.collect(clickEvent);
                        }
                    }
                })
                .print("使用flatMap方式实现filter");

        env.execute();
    }
    public static class MyFilter implements FilterFunction<ClickEvent>{

        @Override
        public boolean filter(ClickEvent clickEvent) throws Exception {
            return clickEvent.username.equals("Mary");
        }
    }
}
