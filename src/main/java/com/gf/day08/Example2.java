package com.gf.day08;

import com.gf.utils.ClickSource;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //每隔10秒钟保存一次检查点
        env.enableCheckpointing(10*1000L);
        //设置保存检查点的文件夹保存路径
        env.setStateBackend(new FsStateBackend("file:\\D:\\IdeaProjects\\bigdataCode\\flink_0906\\src\\main\\resources\\ckpts"));
        env.addSource(new ClickSource()).print();
        env.execute();
    }
}
