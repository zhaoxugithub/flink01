package atguigu.base

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp

object UvCountByWindowExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.addSource(new ClickSource).assignAscendingTimestamps(_.timestamp1)
      // 为所有数据都指定同一个 key，可以将所有数据都发送到同一个分区
      .keyBy(_ => "key")
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .process(new UvCountByWindow)
      .print()
    env.execute()
  }

  // 自定义窗口处理函数
  /*
    ProcessWindowFunction:是窗口函数中的全窗口函数中的处理窗口函数
    全窗口聚合函数需要先收集窗 口中的数据，并在内部缓存起来，等到窗口要输出结果的时候再取出数据进行计算
    elements：就是窗口中缓存的数据
   */
  class UvCountByWindow extends ProcessWindowFunction[Event, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Event], out: Collector[String]): Unit = {
      // 初始化一个 Set 数据结构，用来对用户名进行去重
      var userSet = Set[String]()
      // 将所有用户名进行去重
      elements.foreach(userSet += _.user1)
      // 结合窗口信息，包装输出内容
      val windowStart = context.window.getStart
      val windowEnd = context.window.getEnd
      out.collect(" 窗 口 ： " + new Timestamp(windowStart) + "~" + new Timestamp(windowEnd) + "的独立访客数量是：" + userSet.size)
    }
  }
}

