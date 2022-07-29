package flink01.chapter09;

import flink01.chapter05.ClickSource;
import flink01.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class PeriodicPvExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );
        stream.print("原始数据=");
        // 统计每个用户的pv，每隔10s输出一次结果
        stream.keyBy(user -> user.user)
                .process(new PeriodicPvResult())
                .print();
        env.execute();
    }
    /*
        KeyedProcessFunction 继承自AbstractRichFunction
        所以具有任务的生命周期方法:open,close等
        还有RunTimeContext上下文，

        同时，由于继承了KeyedProcessFunction，所以具有OnTimer定时器触发方法
     */
    public static class PeriodicPvResult extends KeyedProcessFunction<String, Event, String> {
        //定义两个状态，保存当前的pv值，以及定时器时间戳
        ValueState<Long> countState;
        ValueState<Long> timerTsState;

        //只会执行一次
        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs", Long.class));
        }

        //每来一条数据执行一次
        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx,
                                   Collector<String> out) throws Exception {
            Long value1 = countState.value();

            if (value1 == null) {
                countState.update(1L);
            } else {
                countState.update(value1 + 1);
            }

            //注册定时器,10s之后会触发
            ctx.timerService().registerEventTimeTimer(value.timestamp + 10 * 1000L);

            //再将定时器保存到状态中
            timerTsState.update(value.timestamp + 10 * 1000L);
        }

        //定时器触发
        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx,
                            Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey() + "pv:" + countState.value());
            //清空状态
            timerTsState.clear();
        }
    }
}
