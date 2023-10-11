package flink01.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class SourceTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1. 从文件中读取数据
        DataStreamSource<String> stream1 = env.readTextFile("data/input/clicks.csv");
        // 2. 从集合中读取数据
        ArrayList<Integer> nums = new ArrayList<>();
        nums.add(2);
        nums.add(5);
        DataStreamSource<Integer> numStream = env.fromCollection(nums);
        numStream.print("sss");
        // sss:6> 2
        // sss:7> 5
        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary", "./home", 1000L));
        events.add(new Event("Bob", "./cart", 2000L));
        DataStreamSource<Event> stream2 = env.fromCollection(events);
        // 3. 从元素读取数据
        DataStreamSource<Event> stream3 = env.fromElements(new Event("Mary", "./home", 1000L), new Event("Bob", "./cart", 2000L));
        // 4. 从Socket文本流读取
        DataStreamSource<String> stream4 = env.socketTextStream("localhost", 7777);
        stream1.print("1");
        numStream.print("nums");
        stream2.print("2");
        stream3.print("3");
        stream4.print("4");
        env.execute();
    }
}
