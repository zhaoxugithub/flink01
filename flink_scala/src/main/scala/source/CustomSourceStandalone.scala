package source

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.util.Random

//自定义数据源
object CustomSourceStandalone {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val value: DataStream[String] = env.addSource(new ParallelSourceFunction[String] {
      var flag = true
      override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
        val random = new Random()
        while (flag) {
          sourceContext.collect("hello" + random.nextInt(1000))
          Thread.sleep(100)
        }
      }
      override def cancel(): Unit = {
        flag = false;
      }
    }).setParallelism(2)
    value.print()
    env.execute();
  }
}
