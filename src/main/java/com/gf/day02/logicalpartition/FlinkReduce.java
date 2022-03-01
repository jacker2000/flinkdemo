package com.gf.day02.logicalpartition;

import com.gf.utils.IntegerSource;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkReduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        //全局
        env.setParallelism(1);

        env.addSource(new IntegerSource())
                .map(r-> Tuple2.of(r,r))
                .returns(Types.TUPLE(Types.INT,Types.INT))
                .keyBy(r->1)
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) throws Exception {
                        return Tuple2.of(
                                Math.min(value1.f0,value2.f0),
                                Math.max(value1.f1,value2.f1)
                        );
                    }
                })
                .print("min -max:");

        env.addSource(new IntegerSource())
                //对数据转换，key为数据，value为初始次数值1
                .map(r-> Tuple2.of(r,1))
                //对tuple类型数据要进行注解
                .returns(Types.TUPLE(Types.INT,Types.INT))
                .keyBy(r->1)
                /**
                 *  todo
                 *      reduce维护了一张hash表，keyBy指定的key是1，value是累加器，是个元组，
                 *          元组的第一个参数是总和
                 *              第二个参数是次数
                 *          输出是计算平均值
                 */
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) throws Exception {
                        return Tuple2.of(
                                (value1.f0+value2.f0),//输入的历史数据总和
                                (value1.f1+value2.f1) //输入数据的总次数
                        );
                    }
                })
                //没必要做注解，截断后是Integer
                .map(r->(r.f0)/(r.f1))
                .print("平均值:");
        env.execute();
    }
}
