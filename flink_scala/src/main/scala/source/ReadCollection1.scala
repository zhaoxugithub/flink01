package source

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object ReadCollection1 {
  def main(args: Array[String]): Unit = {


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[Int] = env.fromCollection(List(1, 2, 3, 4))

    val stream2: DataStream[Int] = env.fromElements(1, 3, 4, 5, 6)

    stream.print()
    stream2.print()

    env.execute()
  }

}
