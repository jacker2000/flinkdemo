package com.gf.day09;

import com.gf.utils.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;


public class Example7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<UserBehavior> stream = env
                .readTextFile("D:\\IdeaProjects\\bigdataCode\\flink_0906\\src\\main\\resources\\UserBehavior.csv")
                //对文本数据分隔处理成UserBehavior
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        //读取每行数据，通过,分隔
                        String[] arr = value.split(",");
                        return new UserBehavior(
                                arr[0], arr[1], arr[2], arr[3],
                                Long.parseLong(arr[4]) * 1000L
                        );
                    }
                })
                //把数据过滤为类型为pv的数据
                .filter(r -> r.type.equals("pv"))
                //设置水位线时间戳，超时时间为0，且UserBehavior中以ts为时间戳
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                        return element.ts;
                                    }
                                })
                );
        //获取表执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env,settings);

        //将数据流转换成动态表
        Table table = streamTableEnvironment.fromDataStream(
                        stream,
                        $("userId"),
                        $("itemId"),
                        $("categoryId").as("cid"),
                        $("type"),
                        //rowtime表示ts是事件时间
                        $("ts").rowtime()
                );
        //将动态表转换回数据流
//        DataStream<Row> rowDataStream = streamTableEnvironment.toDataStream(table);
//        rowDataStream.print();
        /*
            +I[273098, 4221836, 2520377, pv, 2017-11-26 09:59:59.0]
            +I 表示Insert
         */
        //在动态表上进行查询
        // 计算的是item view count per window
        //将动态表注册为临时视图
        streamTableEnvironment.createTemporaryView("userbehavior",table);
        //hop表示滑动窗口，第一个参数是时间戳的字段名，第二个参数是滑动距离，第三个参数是窗口长度
        //COUNT是全窗口聚合
//        Table result = streamTableEnvironment.sqlQuery(
//                        "select itemId,COUNT(itemId) as cnt," +
//                                "HOP_START(ts,INTERVAL '5' MINUTES,INTERVAL '1' HOURS) as windowStartTime," +
//                                "HOP_END(ts,INTERVAL '5' MINUTES,INTERVAL '1' HOURS) as windowEndTime" +
//                                "FROM userbehavior GROUP BY itemId,HOP(ts,INTERVAL '5' MINUTES,INTERVAL '1' HOURS)"
//                );
//        streamTableEnvironment.toChangelogStream(result).print();

        String innerSql ="select itemId,COUNT(itemId) as cnt," +
                "HOP_START(ts,INTERVAL '5' MINUTES,INTERVAL '1' HOURS) as windowStartTime," +
                "HOP_END(ts,INTERVAL '5' MINUTES,INTERVAL '1' HOURS) as windowEndTime" +
                "FROM userbehavior GROUP BY itemId,HOP(ts,INTERVAL '5' MINUTES,INTERVAL '1' HOURS)";

        //按照窗口结束时间分区，然后按照浏览量降序排列
        String minSql ="SELECT * ,ROW_NUMBER() OVER(PARTITION BY windowEndTime ORDER BY cnt DESC) as row_num"+
                "FROM("+innerSql+")";
        String outerSql ="SELECT * FROM ("+minSql+") where row_num<=3";
        Table result = streamTableEnvironment.sqlQuery(outerSql);
        //当sql查询中存在聚合操作时，必须使用toChangelogStream
        streamTableEnvironment.toChangelogStream(result).print();
        env.execute();
    }

}
