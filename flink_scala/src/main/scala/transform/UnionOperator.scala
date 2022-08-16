package transform

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object UnionOperator {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream1 = env.fromCollection(List(("a", 1), ("b", 2)))
    val stream2 = env.fromCollection(List(("c", 3), ("d", 4)))
    val unionStream = stream1.union(stream2)
    unionStream.print()
    env.execute()
  }
}
