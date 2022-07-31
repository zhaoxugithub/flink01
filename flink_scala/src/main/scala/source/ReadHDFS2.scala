package source

import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object ReadHDFS2 {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val path = "hdfs://centos201:9000/user/hive/warehouse/score/"
    /**
     * 监控hdfs目录，如果有更新，每隔10s 重新读取一次
     */
    val value: DataStream[String] = env.readFile(new TextInputFormat(new Path(path)), path, FileProcessingMode.PROCESS_CONTINUOUSLY, 10)
    value.print("原始数据=")
    env.execute();
  }
}
