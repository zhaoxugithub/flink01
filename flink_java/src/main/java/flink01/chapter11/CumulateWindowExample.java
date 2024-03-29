package flink01.chapter11;

import flink01.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class CumulateWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 读取数据源，并分配时间戳、生成水位线
        SingleOutputStreamOperator<Event> eventStream = env.fromElements(new Event("Alice", "./home", 1000L), new Event("Bob", "./cart", 1000L), new Event("Alice", "./prod?id=1", 25 * 60 * 1000L), new Event("Alice", "./prod?id=4", 55 * 60 * 1000L), new Event("Bob", "./prod?id=5", 3600 * 1000L + 60 * 1000L), new Event("Cary", "./home", 3600 * 1000L + 30 * 60 * 1000L), new Event("Cary", "./prod?id=7", 3600 * 1000L + 59 * 60 * 1000L))
                                                           .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                                                                                                           .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp));
        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 将数据流转换成表，并指定时间属性
        Table eventTable = tableEnv.fromDataStream(eventStream, $("user"), $("url"),
                // 将日志中的时间戳作为时间提取字段，是处理时间
                $("timestamp").rowtime()
                              .as("ts"));
        // 为方便在SQL中引用，在环境中注册表EventTable
        tableEnv.createTemporaryView("EventTable", eventTable);
        // 设置累积窗口，执行SQL统计查询
        Table result = tableEnv.sqlQuery("SELECT " + "user, " + "window_end AS endT, " + "COUNT(url) AS cnt " + "FROM TABLE( " +   // 窗口聚合函数，FROM后面跟着的是一个table
                "CUMULATE( TABLE EventTable, " +    // 定义累积窗口，返回的是一长表
                "DESCRIPTOR(ts), " + "INTERVAL '30' MINUTE, " + // 累积窗口的时间间隔，每隔半小时做一次计算
                "INTERVAL '1' HOUR)) " + // 窗口的最大长度
                "GROUP BY user, window_start, window_end ");
        tableEnv.toDataStream(result)
                .print();
        env.execute();
    }
}
