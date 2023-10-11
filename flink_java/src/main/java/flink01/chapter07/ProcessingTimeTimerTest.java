package flink01.chapter07;

import flink01.chapter05.ClickSource;
import flink01.chapter05.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * 基于处理时间设置定时器
 */
public class ProcessingTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        stream.keyBy(t -> true)
              /*
                    第一个参数：key
                    第二个参数：数据元素
                    第三个参数：返回类型
               */.process(new KeyedProcessFunction<Boolean, Event, String>() {
                  @Override
                  public void processElement(Event value, KeyedProcessFunction<Boolean, Event, String>.Context ctx, Collector<String> out) {
                      // 数据里面的时间戳
                      // Long ctxStamp = ctx.timestamp();
                      // 当前的处理时间
                      long timeServerTime = ctx.timerService()
                                               .currentProcessingTime();
                      // System.out.println("ctxStamp = " + ctxStamp);
                      System.out.println("timeServerTime = " + timeServerTime);
                      out.collect("数据到达，到达时间1" + timeServerTime);
                      // 将时间戳转成日期格式的时间
                      out.collect("数据到达，到达时间2" + new Timestamp(timeServerTime));
                      ctx.timerService()
                         .registerProcessingTimeTimer(timeServerTime + 10 * 1000);
                  }

                  /*
                      靠时间去触发的
                      timestamp：触发时间
                      KeyedProcessFunction<Boolean, Event, String>.OnTimerContext ctx  ==  KeyedProcessFunction<Boolean, Event, String>.Context ctx
                   */
                  @Override
                  public void onTimer(long timestamp, KeyedProcessFunction<Boolean, Event, String>.OnTimerContext ctx, Collector<String> out) {
                      out.collect("定时器触发，触发时间:" + new Timestamp(timestamp) + " 水位线:" + new Timestamp(ctx.timerService()
                                                                                                                    .currentWatermark()));
                  }
              })
              .print();

        env.execute();

    }
}
