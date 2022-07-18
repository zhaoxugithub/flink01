package com.flink01.chapter06;

import com.flink01.chapter05.ClickSource;
import com.flink01.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;


/*
    水位线：前一批数据最大的数据时间戳 - 延迟时间 - 1ms


    如果水位线 > 窗口结束时间就会触发关闭窗口


    延迟到达的数据也会参与计算


    窗口也有一个允许延迟时间，在允许延迟的时间范围内，不会立刻销毁窗口，而是会只参与计算，但是不会销毁窗口！！！


    flink中如何保证数据不丢失：
        1.水位线延迟
        2.窗口允许延迟
        3.迟到数据放到窗口侧边输出流，手动去合并计算！！


 */
public class WatermarkTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 将数据源改为socket文本流，并转换成Event类型
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                // 插入水位线的逻辑
                .assignTimestampsAndWatermarks(
                        // 针对乱序流插入水位线，延迟时间设置为5s
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    // 抽取时间戳的逻辑
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );
                stream.print("原始数据：");
                // 根据user分组，开窗统计
                stream.keyBy(data -> data.user)
                //.countWindow(10,2)//滑动计数窗口10个数统计一次，滑动2一个统计一次
                //.window(EventTimeSessionWindows.withGap(Time.seconds(2))) //事件事件会话窗口
                //.window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(1))):滑动窗口，滑动窗口大小为10s，滑动步长为1s
                //TumblingEventTimeWindows：滚动时间窗口，窗口大小为10s
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new WatermarkTestResult())
                .print();

        env.execute();
    }

    // 自定义处理窗口函数，输出当前的水位线和窗口信息
    public static class WatermarkTestResult extends ProcessWindowFunction<Event, String, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {

            SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            String start = sdf.format(new Date(Long.parseLong(String.valueOf(context.window().getStart()))));
            String end = sdf.format(new Date(Long.parseLong(String.valueOf(context.window().getEnd()))));

            Long currentWatermark = context.currentWatermark();
            Long count = elements.spliterator().getExactSizeIfKnown();
            String user = elements.iterator().next().user;
            out.collect("窗口" + start + " ~ " + end + "中共有" + count + "个元素，user="+user+"窗口闭合计算时，水位线处于：" + currentWatermark);
        }
    }
}
