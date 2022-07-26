package com.flink01.chapter07;

import com.flink01.chapter05.ClickSource;
import com.flink01.chapter05.Event;
import com.flink01.chapter06.UrlViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class KeyedProcessTopN {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        stream.print("原始数据=");

        //先开窗再集合,分区，把相同的url放到一个分区里面
        SingleOutputStreamOperator<UrlViewCount> aggregate = stream.keyBy(t -> t.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                //5s中触发一次
                .aggregate(new MyAgg(), new MyProcessWindowFun());

        aggregate.print();
        env.execute();
    }

    /**
     * 第一个参数：输入参数类型
     * 第二个参数：累加器的参数类型
     * 第三个参数：最终输出的类型
     */
    public static class MyAgg implements AggregateFunction<Event, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    /**
     * * @param <IN> The type of the input value.
     * * @param <OUT> The type of the output value.
     * * @param <KEY> The type of the key.
     * * @param <W> The type of {@code Window} that this window function can be applied on.
     */
    public static class MyProcessWindowFun extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {
        @Override
        public void process(String s, ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>.Context context,
                            Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            /*Timestamp start = new Timestamp(context.window().getStart());
            Timestamp end = new Timestamp(context.window().getEnd());
            String info = "url:" + s + " 浏览量:" + elements.iterator().next() + " 窗口开始时间:" + start
                    + " 窗口结束时间:" + end + " 任务下标：" + indexOfThisSubtask + "\n";*/
            out.collect(new UrlViewCount(s, elements.iterator().next(), context.window().getStart(), context.window().getEnd()));
        }
    }

    /**
     * 自定义函数取TOPN
     * * @param <K> Type of the key.
     * * @param <I> Type of the input elements.
     * * @param <O> Type of the output elements.
     */
    public static class TOPN extends KeyedProcessFunction<Long, UrlViewCount, String> {
        @Override
        public void processElement(UrlViewCount value, KeyedProcessFunction<Long, UrlViewCount, String>.Context ctx,
                                   Collector<String> out) throws Exception {
            //todo 元素排序
        }
    }
}
