package transform

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 *
 * 使用不同的分区策略具有不同的功能：
 *
 * 1.shuffle:随机分区，上游数据，随机分发到下游
 *
 *
 *
 *
 */
object PartitonerTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    val stream = env.generateSequence(1, 20).setParallelism(3)
    println(stream.getParallelism)
    stream.shuffle.print()
    //    stream.rebalance.print()
    //    stream.rescale.print()
    env.execute()
  }
}
