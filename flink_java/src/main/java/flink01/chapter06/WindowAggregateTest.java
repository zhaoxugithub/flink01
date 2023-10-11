package flink01.chapter06;

import flink01.chapter05.ClickSource;
import flink01.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.HashSet;

// 计算pvuv
public class WindowAggregateTest {

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
        // 所有数据设置相同的key，发送到同一个分区统计PV和UV，再相除
        stream.keyBy(data -> true)
              .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
              .aggregate(new AggregateFunction<Event, Tuple2<HashSet<String>, Long>, Double>() {
                  @Override
                  public Tuple2<HashSet<String>, Long> createAccumulator() {
                      return Tuple2.of(new HashSet<String>(), 0L);
                  }

                  @Override
                  public Tuple2<HashSet<String>, Long> add(Event event, Tuple2<HashSet<String>, Long> hashSetLongTuple2) {
                      // 属于本窗口的数据来一条累加一条
                      hashSetLongTuple2.f0.add(event.user);
                      return Tuple2.of(hashSetLongTuple2.f0, hashSetLongTuple2.f1 + 1L);
                  }

                  @Override
                  public Double getResult(Tuple2<HashSet<String>, Long> hashSetLongTuple2) {
                      return (double) hashSetLongTuple2.f1 / hashSetLongTuple2.f0.size();
                  }

                  @Override
                  public Tuple2<HashSet<String>, Long> merge(Tuple2<HashSet<String>, Long> hashSetLongTuple2, Tuple2<HashSet<String>, Long> acc1) {
                      return null;
                  }
              })
              .print();
        env.execute();
    }
}
