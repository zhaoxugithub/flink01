package flink01.chapter09;

import flink01.chapter05.ClickSource;
import flink01.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * 我们举一个简单的例子，对用户点击事件流每 5 个数据统计一次平均时间戳。这是一个类
 * 似计数窗口（CountWindow）求平均值的计算，这里我们可以使用一个有聚合状态的
 * RichFlatMapFunction 来实现。
 */
public class AverageTimestampExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                                                      .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                                                                                      .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp));
        stream.print("原始数据=");
        stream.keyBy(data -> data.user)
              .flatMap(new AvgTsResult())
              .print();
        env.execute();
    }

    public static class AvgTsResult extends RichFlatMapFunction<Event, String> {

        // 设置生存时间10s
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(10))
                                                 // 这里设置了什么时候更新状态失效时间，OnCreateAndWrite 表示无论读写都会更新失效时间
                                                 .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                                 // 这里设置的 NeverReturnExpired 是默认行为，表示从不返回过期值，也就是只要过期就认为它已
                                                 // 经被清除了，应用不能继续读取；这在处理会话或者隐私数据时比较重要。对应的另一种配置
                                                 // 是 ReturnExpireDefNotCleanedUp，就是如果过期状态还存在，就返回它的值
                                                 .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                                 .build();

        // 定义一个聚合状态，用来计算平均时间戳
        AggregatingState<Event, Long> aggregatingState;
        // 定义一个值状态，用来保存当前用户访问频次
        ValueState<Long> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<>("visitCount", Long.class);
            stateDescriptor.enableTimeToLive(ttlConfig);
            valueState = getRuntimeContext().getState(stateDescriptor);
            aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<>("avg-ts", new AggregateFunction<Event, Tuple2<Long, Long>, Long>() {
                @Override
                public Tuple2<Long, Long> createAccumulator() {
                    return Tuple2.of(0L, 0L);
                }

                @Override
                public Tuple2<Long, Long> add(Event value, Tuple2<Long, Long> accumulator) {
                    return Tuple2.of(accumulator.f0 + value.timestamp, accumulator.f1 + 1);
                }

                @Override
                public Long getResult(Tuple2<Long, Long> accumulator) {
                    return accumulator.f0 / accumulator.f1;
                }

                @Override
                public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                    return null;
                }
            }, Types.TUPLE(Types.LONG, Types.LONG)));
        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            Long count = valueState.value();
            if (count == null) {
                count = 1L;
            } else {
                count++;
            }
            valueState.update(count);
            aggregatingState.add(value);

            // 到达5次就输出结果，并且清空状态
            if (count == 5) {
                out.collect(value.user + " 平均时间戳： " + new Timestamp(aggregatingState.get()));
                valueState.clear();
            }
        }
    }
}
