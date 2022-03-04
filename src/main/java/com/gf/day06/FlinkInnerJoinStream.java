package com.gf.day06;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

// SELECT * FROM A INNER JOIN B on A.id = B.id;

/**
 * 结果：
 *    (key-1,1)===>(key-1,5)
 *    (key-2,2)===>(key-2,6)
 *    (key-1,3)===>(key-1,5)
 *    (key-1,1)===>(key-1,7)
 *    (key-1,3)===>(key-1,7)
 *    (key-2,4)===>(key-2,6)
 *    (key-2,2)===>(key-2,8)
 *    (key-2,4)===>(key-2,8)
 */
public class FlinkInnerJoinStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Tuple2<String, Integer>> stream1 = env.fromElements(
                Tuple2.of("key-1", 1),
                Tuple2.of("key-2", 2),
                Tuple2.of("key-1", 3),
                Tuple2.of("key-2", 4)
        );
        DataStreamSource<Tuple2<String, Integer>> stream2 = env.fromElements(
                Tuple2.of("key-1", 5),
                Tuple2.of("key-2", 6),
                Tuple2.of("key-1", 7),
                Tuple2.of("key-2", 8)
        );
        stream1.keyBy(r->r.f0)
                .connect(stream2.keyBy(r->r.f0))
                .process(new InnerJoin())
                .print();

        env.execute();
    }
    public static class  InnerJoin extends CoProcessFunction<Tuple2<String,Integer>,Tuple2<String,Integer>,String>{
        private ListState<Tuple2<String,Integer>>  historydata1;
        private ListState<Tuple2<String,Integer>>  historydata2;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            historydata1= getRuntimeContext().getListState(
                    new ListStateDescriptor<Tuple2<String, Integer>>("list-data1",  Types.TUPLE(Types.STRING,Types.INT))
            );
            historydata2= getRuntimeContext().getListState(
                    new ListStateDescriptor<Tuple2<String, Integer>>("list-data2",  Types.TUPLE(Types.STRING,Types.INT))
            );
        }

        @Override
        public void processElement1(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
            historydata1.add(value);
            for (Tuple2<String, Integer> e : historydata2.get()) {
                out.collect(value+"===>"+e);
            }
        }

        @Override
        public void processElement2(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
            historydata2.add(value);
            for (Tuple2<String, Integer> e : historydata1.get()) {
                out.collect(e+"===>"+value);
            }
        }
    }
}
