package com.flink01.chapter07;

import com.flink01.chapter05.ClickSource;
import com.flink01.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * 基于事件时间设置定时器
 */
public class EventTimeTimerTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                // 插入水位线的逻辑
                .assignTimestampsAndWatermarks(
                        // 针对乱序流插入水位线，延迟时间设置为5s
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    // 抽取时间戳的逻辑
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );
        stream.keyBy(t -> t.user)
                /*
                      第一个参数：key
                      第二个参数：数据元素
                      第三个参数：返回类型
                 */
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
                        //数据里面的时间戳
                        //Long ctxStamp = ctx.timestamp();
                        //当前的处理时间,如果是基于事件时间的话，就不能使用ctx.timerService().currentProcessingTime() 作为到达时间了
                        //long timeServerTime = ctx.timerService().currentProcessingTime();
                        //要用数据里面本身自带的时间戳
                        Long ctxStamp = ctx.timestamp();
                        out.collect(ctx.getCurrentKey() + " 数据到达,到达时间：" + new Timestamp(ctxStamp) + "水位线:" + new Timestamp(ctx.timerService().currentWatermark()));
                        ctx.timerService().registerEventTimeTimer(ctxStamp + 10 * 1000);
                    }
                    /*
                        靠registerEventTimeTimer(ctxStamp + 10 * 1000)里面的时间去触发的
                        timestamp：触发时间
                        KeyedProcessFunction<Boolean, Event, String>.OnTimerContext ctx  ==  KeyedProcessFunction<Boolean, Event, String>.Context ctx
                     */
                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + " 定时器触发，触发时间:" + new Timestamp(timestamp)+" 水位线:" + new Timestamp(ctx.timerService().currentWatermark()));
                    }
                }).print();
        env.execute();

    }
}
