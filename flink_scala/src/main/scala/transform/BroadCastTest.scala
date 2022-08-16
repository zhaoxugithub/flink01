package transform

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

//广播算子
object BroadCastTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[Long] = env.generateSequence(1, 100).setParallelism(2)
    stream.writeAsText("data/stream1").setParallelism(2)
    

  }
}
