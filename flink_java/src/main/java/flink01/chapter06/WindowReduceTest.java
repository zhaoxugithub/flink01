package flink01.chapter06;

import flink01.chapter05.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class WindowReduceTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.socketTextStream("192.168.1.151", 7777)
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
                    }
                })
                //有序流水
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        //提取时间戳策略
                        .withTimestampAssigner((e,l)->e.timestamp)
                );

            stream.map(e-> Tuple2.of(e.user,1L)).returns(Types.TUPLE(Types.STRING,Types.LONG))
                    //分流，相同的key放到同一个分区里面
                    .keyBy(r->r.f0)
                    //开窗，滚动窗口大小为5s
//                    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                    //滑动窗口大小为5s,滑动步长为1s,每一秒计算一次，计算输出在这5s内的数据
                    .window(SlidingEventTimeWindows.of(Time.seconds(5),Time.seconds(1)))
                    //集合函数，当达到5s的时候会聚合一次
                    .reduce((t1,t2)->Tuple2.of(t1.f0,t1.f1+t2.f1)).returns(Types.TUPLE(Types.STRING,Types.LONG))
                    .print();
            env.execute();
    }
}
