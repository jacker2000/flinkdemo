package com.gf.day04;

import com.alibaba.fastjson.JSON;
import com.gf.utils.ClickEvent;
import com.gf.utils.ClickSource;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 *  todo
 *      MapStata例子
 *     每来一条数据，输出一次这个用户对每个url的访问次数hashmap
 */
public class FlinkMapState_KeyedProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .keyBy(r->r.username)
                .process(new KeyedProcessFunction<String, ClickEvent, String>() {
                    /**
                     *  key:url
                     *  value:url访问次数
                     */
                    private MapState<String,Long> mapState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        mapState=getRuntimeContext().getMapState(
                                 new MapStateDescriptor<String, Long>(
                                         "mapstata", Types.STRING,Types.LONG
                                 )
                        );
                    }

                    @Override
                    public void processElement(ClickEvent value, Context ctx, Collector<String> out) throws Exception {
                        //如果当前数据中的url之前没有访问过，那么统计值加1
                        if (!mapState.contains(value.url)) {
                            mapState.put(value.url,1L);
                        }
                        //如果当前数据中的url之前访问过，那么统计值加1
                        else {
                            mapState.put(value.url,mapState.get(value.url)+1L);
                        }
                        //格式化输出方式一： json串方式
                        Map map =new HashMap<String, Integer>();

                        StringBuilder result = new StringBuilder();

                        for (String url : mapState.keys()) {
                            map.put(url,mapState.get(url));
                        }
                        //todo mapState没有被清空，导致mapState会越来越大，导致OOM
                        String mapJson = JSON.toJSONString(map);
                        result.append(ctx.getCurrentKey()+"\n").append(mapJson);

                        //格式化输出方式二： 字符串方式
//                        result.append(ctx.getCurrentKey()+"=>{ \n");
//                        for (String url : mapState.keys()) {
//                            result.append(" "+url+": "+mapState.get(url)+" ;\n");
//                        }
//                        result.append("} \n");

//                        out.collect(result.toString());
                        out.collect(result.toString());
                    }
                })
                .print();

        env.execute();
    }
}
