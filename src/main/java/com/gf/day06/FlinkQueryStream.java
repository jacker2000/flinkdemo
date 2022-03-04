package com.gf.day06;

import com.gf.utils.ClickEvent;
import com.gf.utils.ClickSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 *  查询流
 */
public class FlinkQueryStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<ClickEvent> clickStream = env.addSource(new ClickSource());
        DataStreamSource<String> queryStream = env.socketTextStream("localhost", 9999);
        clickStream.keyBy(r->r.url)
                //广播之前将数据流的并行度设置为1
        .connect(queryStream.setParallelism(1).broadcast())
                .flatMap(new Query())
                .print();
        env.execute();
    }
    public static class Query implements CoFlatMapFunction<ClickEvent,String,ClickEvent> {
        private String queryString="";

        @Override
        public void flatMap1(ClickEvent value, Collector<ClickEvent> out) throws Exception {
            if (value.username.equals(queryString)) {
                out.collect(value);
            }
        }

        @Override
        public void flatMap2(String value, Collector<ClickEvent> out) throws Exception {
            queryString=value;
        }
    }
}
