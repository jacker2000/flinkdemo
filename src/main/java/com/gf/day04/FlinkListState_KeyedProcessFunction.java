package com.gf.day04;

import com.gf.utils.IntegerSource;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 *  ListState 举例
 *  每来一条数据，将历史数据进行排序输出
 */
public class FlinkListState_KeyedProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new IntegerSource())
                .keyBy(r->1)
                .process(new SortHistory())
                .print();

        env.execute();
    }
    public static class SortHistory extends KeyedProcessFunction<Integer,Integer,String> {

        private ListState<Integer> historyData;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            historyData= getRuntimeContext().getListState(
                    new ListStateDescriptor<Integer>("history-data", Types.INT)
            );
        }

        @Override
        public void processElement(Integer value, Context ctx, Collector<String> out) throws Exception {
            historyData.add(value);
            //将列表状态变量中的数据取出后然后放进ArrayList
             List<Integer> integers = new ArrayList<>();
            for (Integer i : historyData.get()) {
                integers.add(i);
            }
            //升序排序
            integers.sort(new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    return o1-o2;
                }
            });
            //格式化一个字符串
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < integers.size()-1; i++) {
                result.append(integers.get(i)+"->");
            }
            result.append(integers.get(integers.size()-1));
            out.collect(result.toString());
        }
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);

        }
    }
}
