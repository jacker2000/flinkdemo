package com.gf.day08;

import com.gf.utils.ClickSource;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkFileCheckpoint {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //每隔10秒钟保存一次检查点,只保留最新的检查点，旧的检查点删除
        env.enableCheckpointing(10*1000L);
        //设置保存检查点的文件夹保存路径
        //file://+ 文件夹的绝对路径
        env.setStateBackend(new FsStateBackend("file:\\D:\\IdeaProjects\\bigdataCode\\flink_0906\\src\\main\\resources\\ckpts"));
        env.addSource(new ClickSource()).print();
        env.execute();
    }
}
