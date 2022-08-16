package transform

import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.collection.mutable


/**
 * 现有一个配置文件存储车牌号与车主的真实姓名，\
 * 通过数据流中的车牌号实时匹配出对应的车主姓名
 * （注意：配置文件可能实时改变）
 * 配置文件可能实时改变  读取配置文件的适合  readFile  readTextFile（读一次）
 * stream1.connect(stream2)
 */
object CoFlatMap {

  def main(args: Array[String]): Unit = {


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    val socketStream: DataStream[String] = env.socketTextStream("localhost", 9991)

    val path = "data/input/carddict.txt"
    val fileStream: DataStream[String] = env.readFile(new TextInputFormat(new Path(path)), path, FileProcessingMode.PROCESS_CONTINUOUSLY, 10)


    socketStream.connect(fileStream)
      //String,String,String ===> IN,IN,OUT
      .map(new CoMapFunction[String, String, String] {
        //这里的map只会在fileStream 数据所在的分区有效
        private val hashMap = new mutable.HashMap[String, String]()

        //处理的是socketStream
        override def map1(in1: String): String = {
          hashMap.getOrElseUpdate(in1, "not found name");
        }

        override def map2(in2: String): String = {
          val strings: Array[String] = in2.split(" ")
          hashMap.put(strings(0), strings(1))
          in2 + "加载完毕..."
        }
      }).print()
    env.execute()
  }

}
