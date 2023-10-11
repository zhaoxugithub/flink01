package flink01.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class SourceKafkaTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "yun:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        // 生成数据
        //./kafka-console-producer.sh --broker-list localhost:9092 --topic "clicks"
        // FlinkKafkaConsumer 最上层实现了SourceFunction接口
        DataStreamSource<String> stream = env.addSource(new FlinkKafkaConsumer<>("clicks",
                // SimpleStringSchema 是一个内置的DeserializationSchema，将字节数组简单的反序列化成一个字符串
                new SimpleStringSchema(), properties));
        stream.print("Kafka");

        env.execute();
    }
}
