package source

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties
import scala.io.Source

object SendToKafka {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val props = new Properties()
    //注意   sparkstreaming + kafka（0.10之前版本） receiver模式  zookeeper url（元数据）
    props.setProperty("bootstrap.servers", "1.15.149.196:9092")
    //设置key和value的序列化器
    props.setProperty("key.serializer", classOf[StringSerializer].getName)
    props.setProperty("value.serializer", classOf[StringSerializer].getName)
    val producer = new KafkaProducer[String,String](props)
    //不要轻易调用getLines
    val iterator = Source.fromFile("data/input/carFlow_all_column_test.txt").getLines()
    for (i <- 1 to 100) {
      for (elem <- iterator) {
        println(elem)
        //kv mq
        val splits = elem.split(",")
        val monitorId = splits(0).replace("'","")
        val carId = splits(2).replace("'","")
        val timestamp = splits(4).replace("'","")
        val speed = splits(6)
        val builder = new StringBuilder
        val info = builder.append(monitorId + "\t").append(carId + "\t").append(timestamp + "\t").append(speed)
        producer.send(new ProducerRecord[String,String]("flink-kafka",i+"",info.toString))
      }
    }
  }
}
