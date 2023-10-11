package flink01.chapter07;


import flink01.chapter05.ClickSource;
import flink01.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/*
    需求：
        1.需要统计最近的10秒钟内最热门的两个url链接，并且每5秒钟更新一次
 */
public class ProcessAllWindowTopN {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                                                      .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                                                                                      .withTimestampAssigner((SerializableTimestampAssigner<Event>) (event, l) -> event.timestamp));
        stream.print("原始数据=");
        stream.keyBy(t -> t.url);
        SingleOutputStreamOperator<String> result = stream.map(t -> t.url)
                                                          .returns(Types.STRING)
                                                          // 设置并行度为1的窗口，上游的所有分区数据都
                                                          .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                                                          .process(new ProcessAllWindowFunction<String, String, TimeWindow>() {
                                                              @Override
                                                              public void process(ProcessAllWindowFunction<String, String, TimeWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                                                                  HashMap<String, Long> map = new HashMap<>();
                                                                  for (String url : elements) {
                                                                      if (map.containsKey(url)) {
                                                                          long newNum = map.get(url) + 1;
                                                                          map.put(url, newNum);
                                                                      } else {
                                                                          map.put(url, 1L);
                                                                      }
                                                                  }
                                                                  // 将map 放到list中
                                                                  LinkedList<Map.Entry<String, Long>> list = new LinkedList<Map.Entry<String, Long>>();
                                                                  for (Map.Entry<String, Long> stringLongEntry : map.entrySet()) {
                                                                      list.push(stringLongEntry);
                                                                  }
                                                                  // 排序
                                                                  list.sort(Comparator.comparingInt(o -> o.getValue()
                                                                                                          .intValue()));
                                                                  StringBuilder sb = new StringBuilder();
                                                                  sb.append("---------------------------------\n");
                                                                  for (int i = 0; i < list.size(); i++) {
                                                                      String info = "url:" + list.get(i)
                                                                                                 .getKey() + " 浏览量:" + list.get(i)
                                                                                                                              .getValue() + " 窗口开始时间:" + new Timestamp(context.window()
                                                                                                                                                                                    .getStart()) + " 窗口结束时间:" + new Timestamp(context.window()
                                                                                                                                                                                                                                           .getEnd()) + "\n";
                                                                      sb.append(info);
                                                                  }
                                                                  sb.append("---------------------------------\n");
                                                                  out.collect(sb.toString());
                                                              }

                                                          });

        result.print();
        env.execute();
    }
}
