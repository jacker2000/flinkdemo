package com.gf.test.day04;

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

//历史数据排序
public class Example1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new IntegerSource())
                .keyBy(r->1)
                .process(new HistorySort())
                .print();
        env.execute();
    }
    public static class HistorySort extends KeyedProcessFunction<Integer,Integer,String> {
        private ListState<Integer> historyData;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            historyData =getRuntimeContext().getListState(
                    new ListStateDescriptor<Integer>(
                            "history-data",
                            Types.INT
                    )
            );
        }

        @Override
        public void processElement(Integer value, Context ctx, Collector<String> out) throws Exception {
            historyData.add(value);
            //对数据排序
             List<Integer> resultList = new ArrayList<>();
            for (Integer e : historyData.get()) {
                resultList.add(e);
            }
            resultList.sort(new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    return o1-o2;
                }
            });
            //格式化输出
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < resultList.size() - 1; i++) {
                result.append(resultList.get(i)+"=>");
            }
            result.append(resultList.get(resultList.size()-1));

            out.collect(result.toString());
        }
    }
}
