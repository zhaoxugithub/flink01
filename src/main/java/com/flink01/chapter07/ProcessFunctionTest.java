package com.flink01.chapter07;

import com.flink01.chapter05.ClickSource;
import com.flink01.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

public class ProcessFunctionTest {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置生成水位线的时间间隔
        env.getConfig().setAutoWatermarkInterval(200);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        stream.print("原始数据=");
        stream.process(new ProcessFunction<Event, String>() {

            /**
             *
             * @param event The input value.
             * @param ctx A {@link Context} that allows querying the timestamp of the element and getting a
             *     {@link TimerService} for registering timers and querying the time. The context is only
             *     valid during the invocation of this method, do not store it.
             *
             *            timestamp,timerService,output(侧边输出流)
             *
             *
             *
             * @param out The collector for returning result values.
             * @throws Exception
             */
            //每来一条数据执行一次
            @Override
            public void processElement(Event event, ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {
                if ("Marry".equals(event.user)) {
                    out.collect(event.user + "click:" + event.url);
                } else if ("Bob".equals(event.user)) {
                    out.collect(event.user);
                    out.collect(event.user);
                }
                out.collect(event.toString());
                //获取当前的时间
                Long timestamp = ctx.timestamp();
                //ctx.output(); //侧边输出流
                //ctx.timerService(); //获取定时器服务
                //因为processFunction又继承了RichFunction，所以他又具备了富函数的相关功能
                //获取运行时的子任务下标
                System.out.println("getRuntimeContext().getIndexOfThisSubtask() = " + getRuntimeContext().getIndexOfThisSubtask());
                //获取任务的处理状态
                //getRuntimeContext().getState()
                TimerService timerService = ctx.timerService();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                String start = sdf.format(new Date(Long.parseLong(String.valueOf(timestamp))));
                String end = sdf.format(new Date(Long.parseLong(String.valueOf(timerService.currentWatermark()))));
                System.out.println("日志事件时间 = " + start);
                System.out.println("水位线 = " + end);
            }
        });
        env.execute();
    }
}
