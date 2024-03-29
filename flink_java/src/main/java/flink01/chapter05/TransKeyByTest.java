package flink01.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransKeyByTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Event> eventDataStreamSource = env.fromElements(new Event("mary", "./home", 1000L), new Event("Bob", "./cart", 2000L), new Event("mary", "./home", 3000L));
        // 使用lambda
        eventDataStreamSource.keyBy(e -> e.user)
                             .print();
        System.out.println("----------");
        // 使用匿名事先keySelector
        //        eventDataStreamSource.keyBy(new KeySelector<Event, Object>() {
        //            @Override
        //            public Object getKey(Event value) throws Exception {
        //                return value.user;
        //            }
        //        }).print();
        env.execute();
    }
}
