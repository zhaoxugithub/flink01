package transform

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
 * 侧输出流
 */
object SideOutputOperator {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[String] = env.socketTextStream("localhost", 9991)
    //定义侧输出流的tag标签
    val gtTag = new OutputTag[String]("gt")
    //输入为String，输出：String
    val processStream: DataStream[String] = stream.process(new ProcessFunction[String, String] {
      override def processElement(i: String, context: ProcessFunction[String, String]#Context, collector: Collector[String]): Unit = {
        try {
          val lonVar: Long = i.toLong
          if (lonVar > 100) {
            collector.collect(i)
          } else {
            context.output(gtTag, i)
          }
        } catch {
          case e => e.printStackTrace()
            context.output(gtTag, i)
        }
      }
    })
    processStream.getSideOutput(gtTag).print("sideStream=")
    processStream.print("mainStream=")
    env.execute()
  }
}
