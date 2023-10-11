package flink01.chapter06;

import flink01.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;


/*



"zs","/home1",1658034285000
"zs","/home1",1658034285010
"zs","/home1",1658034285020
"lisi","/home1",1658034285100
"lisi","/home1",1658034285150
"lisi","/home1",1658034285200
"lisi","/home1",1658034285250
"wangwu","/home1",1658034285300
"wangwu","/home1",1658034295000


窗口1658034280000 ~ 1658034290000中共有3个元素，窗口闭合计算时，水位线处于：1658034289999
窗口1658034280000 ~ 1658034290000中共有4个元素，窗口闭合计算时，水位线处于：1658034289999
窗口1658034280000 ~ 1658034290000中共有1个元素，窗口闭合计算时，水位线处于：1658034289999
 */
public class WatermarkTest02 {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置水位线时间周期，每100ms设置一个水位线
        env.getConfig()
           .setAutoWatermarkInterval(100);
        env.socketTextStream("192.168.1.151", 7777)
           .map(new MapFunction<String, Event>() {
               @Override
               public Event map(String value) throws Exception {
                   String[] fields = value.split(",");
                   return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
               }
           })
           .assignTimestampsAndWatermarks(
                   // 无序流的水位生成
                   // Duration.ofSeconds(5) 延迟5s
                   WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                    // 时间戳的提取规则
                                    .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                        @Override
                                        public long extractTimestamp(Event element, long recordTimestamp) {
                                            return element.timestamp;
                                        }
                                    }))
           .keyBy(data -> data.user)
           .window(TumblingEventTimeWindows.of(Time.seconds(10)))
           .process(new WatermarkTestResult())
           .print();
        env.execute();
    }

    // 自定义处理窗口函数，输出当前的水位线和窗口信息
    public static class WatermarkTestResult extends ProcessWindowFunction<Event, String, String, TimeWindow> {
        // 这个函数直到第10s数据来的时候才会触发
        @Override
        public void process(String s, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            Long start = context.window()
                                .getStart();
            Long end = context.window()
                              .getEnd();
            Long currentWatermark = context.currentWatermark();
            Long count = elements.spliterator()
                                 .getExactSizeIfKnown();
            out.collect("窗口" + start + " ~ " + end + "中共有" + count + "个元素，窗口闭合计算时，水位线处于：" + currentWatermark);
        }
    }
}
