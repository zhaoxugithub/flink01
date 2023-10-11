package atguigu.base

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object ShuffleTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    // 读取数据源，并行度为 1
    val stream = env.addSource(new ClickSource)
    //随机配分服从均匀分布 经洗牌后打印输出，并行度为 4,
    //stream.shuffle.print("shuffle").setParallelism(4)
    //轮询分布，1，2，3，4｜ 1，2，3，4｜1，2，3，4
    //stream.rebalance.print("rebalance").setParallelism(4)
    //重缩放，比较一下轮询分布和重缩放之间的区别
    //stream.rescale.print("rescale").setParallelism(4)
    //广播之后所有的分区都会存在
    stream.broadcast.print("broadcast").setParallelism(4)
    env.execute()
  }
}
