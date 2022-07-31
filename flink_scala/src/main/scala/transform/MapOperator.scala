package transform

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.collection.mutable.ListBuffer

object MapOperator {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    val stream: DataStream[Long] = env.generateSequence(1, 100)
    val stream: DataStream[String] = env.socketTextStream("localhost", 8888)
    stream.map(x => x + "------").print()

    //如何使用flatMap 代替filter

    //数据中如果包含了abc那么这条数据就过滤掉

    stream.flatMap(x => {
      val rest = new ListBuffer[String]
      if (x.contains("abc")) {
        rest += x;
      }
      rest.iterator
    }).print()

    //等同于



    //stream.print()
    env.execute()
  }
}
