package com.gf.day07;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

//CoProcessFunction
// select * from A inner join B on A.id=B.id;
public class FlinkTwoTupleJoin<T extends Tuple, T1 extends Tuple, S> {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Tuple2<String, Integer>> stream1 = env
                .fromElements(
                        Tuple2.of("a", 1),
                        Tuple2.of("b", 2),
                        Tuple2.of("c", 3),
                        Tuple2.of("d", 4),
                        Tuple2.of("a", 5)
                );
        DataStreamSource<Tuple2<String, Integer>> stream2 = env
                .fromElements(
                        Tuple2.of("a", 5),
                        Tuple2.of("b", 6),
                        Tuple2.of("c", 7),
                        Tuple2.of("d", 8),
                        Tuple2.of("a", 9)
                );

        stream1.keyBy(r->r.f0)
                .connect(stream2.keyBy(r->r.f0))
                .process(new InnerJoin())
                .print();
        env.execute();
    }
    //
    public static class InnerJoin extends CoProcessFunction<Tuple2<String, Integer>,Tuple2<String, Integer>,String> {

        //保存第一条历史数据
        private ListState<Tuple2<String, Integer>> historydata1;
        //保存第二条历史数据
        private ListState<Tuple2<String, Integer>> historydata2;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            historydata1=getRuntimeContext().getListState(
                    new ListStateDescriptor<Tuple2<String, Integer>>(
                            "historydata-1",
                            Types.TUPLE(Types.STRING,Types.INT)
                    )
            );
            historydata2=getRuntimeContext().getListState(
                    new ListStateDescriptor<Tuple2<String, Integer>>(
                            "historydata-2",
                            Types.TUPLE(Types.STRING,Types.INT)
                    )
            );

        }

        @Override
        public void processElement1(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
            historydata1.add(value);
            //当前数据和第二条流的所有历史数据进行join
            for (Tuple2<String, Integer> e : historydata2.get()) {
                out.collect(value+"=>"+e);
            }
        }

        @Override
        public void processElement2(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
            historydata2.add(value);
            //当前数据和第一条流的所有历史数据进行join
            for (Tuple2<String, Integer> e : historydata1.get()) {
                out.collect(value+"=>"+e);
            }
        }
    }
}
