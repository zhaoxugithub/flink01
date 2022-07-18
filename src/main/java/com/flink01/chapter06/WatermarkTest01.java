package com.flink01.chapter06;

import com.flink01.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 有序流，无序流的水位线生成
 */
public class WatermarkTest01 {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置水位线时间周期，每100ms设置一个水位线
        env.getConfig().setAutoWatermarkInterval(100);
//        env.fromElements(
//                        new Event("Bob", "./cart", 2000L),
//                        new Event("Alice", "./prod?id=100", 3000L),
//                        new Event("Alice", "./prod?id=200", 3500L),
//                        new Event("Bob", "./prod?id=2", 2500L),
//                        new Event("Alice", "./prod?id=300", 3600L),
//                        new Event("Bob", "./home", 3000L),
//                        new Event("Bob", "./prod?id=1", 2300L),
//                        new Event("Bob", "./prod?id=3", 3300L))
                env.socketTextStream("192.168.1.151", 7777)
                        .map(new MapFunction<String, Event>() {
                            @Override
                            public Event map(String value) throws Exception {
                                String[] fields = value.split(",");
                                return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
                            }
                        })
                    //有序流的waterMark(水位线)的生成
                    .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                            //定义怎么样提取时间戳
                            .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                @Override
                                public long extractTimestamp(Event element, long recordTimestamp) {
                                    return element.timestamp;
                                }
                            })).print();

             /*   .assignTimestampsAndWatermarks(
                        //无序流的水位生成
                        //Duration.ofSeconds(5) 延迟5s
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                //时间戳的提取规则
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                ).print();*/

        env.execute();
    }
}
