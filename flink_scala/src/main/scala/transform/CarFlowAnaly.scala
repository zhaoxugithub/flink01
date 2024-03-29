package transform

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTuple2TypeInformation, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

object CarFlowAnaly {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val props = new Properties()
    //注意   sparkstreaming + kafka（0.10之前版本） receiver模式  zookeeper url（元数据）
    props.setProperty("bootstrap.servers", "1.15.149.196:9092")
    props.setProperty("group.id", "flink-kafka-01")
    //设置key和value的序列化器
    props.setProperty("key.deserializer", classOf[StringSerializer].getName)
    props.setProperty("value.deserializer", classOf[StringSerializer].getName)
    val stream: DataStream[(String, String)] = env.addSource(new FlinkKafkaConsumer[(String, String)]("flink-kafka", new KafkaDeserializationSchema[(String, String)] {
      //停止消费数据的条件
      override def isEndOfStream(t: (String, String)): Boolean = false

      // 要进行序列化的字节流
      override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): (String, String) = {
        val key = new String(consumerRecord.key(), "utf-8")
        val value = new String(consumerRecord.value(), "utf-8")
        (key, value)
      }

      //指定要返回的数据类型，Flink提供的类型
      override def getProducedType: TypeInformation[(String, String)] = {
        createTuple2TypeInformation(createTypeInformation[String], createTypeInformation[String])
      }
    }, props)
    )
    val value: DataStream[(String, Int)] = stream.map(x => {
        val monitorID = x._2.split(" ")(0)
        (monitorID, 1)
      }).keyBy(0)
      .sum(1)
    println("-------------------------------------")
    val valueStream = stream.map(_._2)
    //stream 中元素类型 变成二元组类型  kv stream   k:monitor_id v:1
    env.execute()
  }
}
