package atguigu.base

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream: DataStream[String] = env.socketTextStream("centos201", 9999)
    val result: DataStream[(String, Int)] = dataStream
      .flatMap(x => x.split(" "))
      .map(x => (x, 1))
      .keyBy(_._1)
      .sum(1)
    result.print("result=")
    env.execute();
  }
}
