package flink01.chapter06;

import flink01.chapter05.ClickSource;
import flink01.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

public class UvCountByWindowExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                                                      .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                                                                                      .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp));
        // 将数据全部发往同一分区，按窗口统计UV
        stream.keyBy(data -> true)
              .window(TumblingEventTimeWindows.of(Time.seconds(10)))
              .process(new ProcessWindowFunction<Event, String, Boolean, TimeWindow>() {
                  @Override
                  public void process(Boolean aBoolean, ProcessWindowFunction<Event, String, Boolean, TimeWindow>.Context context, Iterable<Event> elements, Collector<String> out) {
                      HashSet<Object> userSet = new HashSet<>();
                      for (Event element : elements) {
                          userSet.add(element.user);
                      }
                      // 结合窗口信息，包装输出内容
                      long start = context.window()
                                          .getStart();
                      long end = context.window()
                                        .getEnd();
                      out.collect("窗口: " + new Timestamp(start) + " ~ " + new Timestamp(end) + " 的独立访客数量是：" + userSet.size());
                  }
              })
              .print();
        env.execute();
    }
}
