package com.gf.test.day04;

import com.gf.utils.ClickEvent;
import com.gf.utils.ClickSource;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import scala.Int;

//每个用户访问每个url具体信息
public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .keyBy(r->r.username)
                .process(new KeyedProcessFunction<String, ClickEvent, String>() {
                    private MapState<String , Integer> urlCount;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        urlCount= getRuntimeContext().getMapState(
                                new MapStateDescriptor<String, Integer>(
                                        "url-count",
                                        Types.STRING,
                                        Types.INT
                                )
                        );
                    }

                    @Override
                    public void processElement(ClickEvent value, Context ctx, Collector<String> out) throws Exception {
                        //第一次访问
                        if (!urlCount.contains(value.url)) {
                            urlCount.put(value.url,1);
                        }
                        //不是第一次访问
                        else{
                            urlCount.put(value.url,urlCount.get(value.url)+1);
                        }
                        //格式化输出
                        StringBuilder result= new StringBuilder();
                        result.append(value.username+"=> { \n");
                        for (String url : urlCount.keys()) {
                            result.append(""+url+"->"+urlCount.get(url)+"\n");
                        }
                        result.append("} \n");
                        out.collect(result.toString());
                    }
                })
                .print();

        env.execute();
    }
}
