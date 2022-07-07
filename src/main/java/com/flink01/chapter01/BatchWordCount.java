package com.flink01.chapter01;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

//基于DataSet 批处理的方式执行，但是官方已经不推荐使用了
public class BatchWordCount {

    public static void main(String[] args) throws Exception {
        //创建执行环境
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> stringDataSource = executionEnvironment.readTextFile("data/input/word.txt");
        FlatMapOperator<String, Tuple2<String, Long>> flatMapOperator = stringDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect((Tuple2.of(word, 1L)));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));//当Lambda表达式使用 Java 泛型的时候, 由于泛型擦除的存在, 需要显示的声明类型信息
        UnsortedGrouping<Tuple2<String, Long>> tuple2UnsortedGrouping = flatMapOperator.groupBy(0);
        //对元组中的第二个元素进行求和
        AggregateOperator<Tuple2<String, Long>> sum = tuple2UnsortedGrouping.sum(1);
        //print 抛出异常
        sum.print();
    }
}
