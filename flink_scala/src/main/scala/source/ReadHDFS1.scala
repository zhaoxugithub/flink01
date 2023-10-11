package source

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object ReadHDFS1 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[String] = env.readTextFile("hdfs://centos201:9000/user/hive/warehouse/score/")
    val value: DataStream[(String, String, String)] = stream.map(line => {
      val strings: Array[String] = line.split("\t")
      (strings(0), strings(1), strings(2))
    })
    value.print("from hdfs=")
    env.execute("hdfs job")
  }
}
