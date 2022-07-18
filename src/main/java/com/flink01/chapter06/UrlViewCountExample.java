package com.flink01.chapter06;

import com.flink01.chapter05.ClickSource;
import com.flink01.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class UrlViewCountExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));
        stream.print("stream=");
        // 需要按照url分组，开滑动窗口统计
        stream.keyBy(data -> data.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                // 同时传入增量聚合函数和全窗口函数
                .aggregate(new UrlViewCountAgg(), new UrlViewCountResult())
                .print();

        env.execute();
    }

    /*
      public String user;
    public String url;
    public Long timestamp;

     */
    public static class UrlViewCountAgg implements AggregateFunction<Event, Long, Long> {

        /**
         * @return
         */
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        /**
         * @param event
         * @param aLong
         * @return
         */
        @Override
        public Long add(Event event, Long aLong) {
            return aLong + 1L;
        }

        /**
         * @param aLong
         * @return
         */
        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        /**
         * @param aLong
         * @param acc1
         * @return
         */
        @Override
        public Long merge(Long aLong, Long acc1) {
            return null;
        }
    }

    // 自定义窗口处理函数，只需要包装窗口信息
    public static class UrlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {
        /**
         * Evaluates the window and outputs none or several elements.
         *
         * @param s        The key for which this window is evaluated.
         * @param context  The context in which the window is being evaluated.
         * @param elements The elements in the window being evaluated.
         * @param out      A collector for emitting elements.
         * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
         */
        @Override
        public void process(String s, ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
            // 结合窗口信息，包装输出内容
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            // 迭代器中只有一个元素，就是增量聚合函数的计算结果
            out.collect(new UrlViewCount(s, elements.iterator().next(), start, end));
        }
    }

}
