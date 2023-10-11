import org.apache.flink.streaming.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    //准备环境
    /**
     * createLocalEnvironment 创建一个本地执行的环境  local
     * createLocalEnvironmentWithWebUI 创建一个本地执行的环境  同时还开启Web UI的查看端口  8081
     * getExecutionEnvironment 根据你执行的环境创建上下文，比如local  cluster
     */
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val value: DataStream[String] = env.socketTextStream("localhost", 9991)
    value.
      flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)
      .print("result:");
    env.execute("first Job")

    /**
     * 6> (msb,1)
     * 1> (,,1)
     * 3> (hello,1)
     * 3> (hello,2)
     * 6> (msb,2)
     * 默认就是有状态的计算
     * 6>  代表是哪一个线程处理的
     * 相同的数据一定是由某一个thread处理
     */
  }
}
