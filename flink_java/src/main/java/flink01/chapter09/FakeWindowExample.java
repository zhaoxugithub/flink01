package flink01.chapter09;

import flink01.chapter05.ClickSource;
import flink01.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * 也就是我们要用映射状态来完整模拟窗口的功能。这里我们模拟一个
 * 滚动窗口。我们要计算的是每一个 url 在每一个窗口中的 pv 数据。我们之前使用增量聚合和
 * 全窗口聚合结合的方式实现过这个需求。
 */
public class FakeWindowExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                })
        );

        stream.print("原始数据=");
        stream.keyBy(data -> data.url)
                .process(new FakeWindowsResult(10000L))
                .print();
        env.execute();

    }

    public static class FakeWindowsResult extends KeyedProcessFunction<String, Event, String> {


        private Long windowSize;


        public FakeWindowsResult(Long windowSize) {
            this.windowSize = windowSize;
        }

        //申明状态，用map保存pv值(窗口的start,end)
        MapState<Long, Long> windowPvMapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            windowPvMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("window-ppv", Long.class, Long.class));
        }

        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
            //每来一条数据，就根据时间戳判断属于哪个窗口
            //value.timestamp 表述日志数据的时间戳
            Long windowsStart = value.timestamp / windowSize * windowSize;
            Long windowEnd = windowsStart + windowSize;

            //注册end-1的定时器，窗口触发计算
            ctx.timerService().registerEventTimeTimer(windowEnd - 1);

            if (windowPvMapState.contains(windowEnd)) {
                Long pv = windowPvMapState.get(windowEnd);
                windowPvMapState.put(windowEnd, pv + 1L);
            } else {
                windowPvMapState.put(windowEnd, 1L);
            }
        }

        //定时器触发
        //timestamp 表示的是窗口触发的时间
        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            Long windowEnd = timestamp + 1;
            Long windowStart = windowEnd - windowSize;
            Long pv = windowPvMapState.get(windowEnd);
            out.collect("url: " + ctx.getCurrentKey()
                    + " 访问量: " + pv
                    + " 窗 口 ： " + new Timestamp(windowStart) + " ~ " + new
                    Timestamp(windowEnd));
            // 模拟窗口的销毁，清除 map 中的 key
            windowPvMapState.remove(windowEnd);
        }
    }

}
