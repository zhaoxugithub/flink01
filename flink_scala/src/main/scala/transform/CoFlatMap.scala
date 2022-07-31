package transform

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


/**
 * 1.stream1.connect(stream2)
 */
object CoFlatMap {

  def main(args: Array[String]): Unit = {


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val socketStream: DataStream[String] = env.socketTextStream("1.15.149.196", 9991)



  }

}
