package flink01.chapter01;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class BoundedStreamWordCount {

    public static void main(String[] args) throws Exception {
        // 创建流式处理环境
        StreamExecutionEnvironment executionEnvironment = StreamContextEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dss = executionEnvironment.readTextFile("data/input/word.txt");
        // xxx operator 表示返回的是转换算子的操作
        SingleOutputStreamOperator<Tuple2<String, Long>> flatMapDSS = dss.flatMap((String line, Collector<String> out) -> {
                                                                             // Arrays.stream(line.split(" ")).forEach((word) -> out.collect(word));
                                                                             // 简化成：
                                                                             Arrays.stream(line.split(" "))
                                                                                   .forEach(out::collect);
                                                                         })
                                                                         .returns(Types.STRING)
                                                                         .map(word -> Tuple2.of(word, 1L))
                                                                         .returns(Types.TUPLE(Types.STRING, Types.LONG));
        // keyBy 并不是一个转换算子，他只是一个数据分区操作
        KeyedStream<Tuple2<String, Long>, String> tuple2StringKeyedStream = flatMapDSS.keyBy(t -> t.f0);
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = tuple2StringKeyedStream.sum(1);
        sum.print();
        executionEnvironment.execute();
    }
}
