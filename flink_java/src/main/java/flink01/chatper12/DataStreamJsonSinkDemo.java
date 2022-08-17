package flink01.chatper12;

public class DataStreamJsonSinkDemo {

    public static void main(String[] args) throws Exception {

     /*    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties pro = new Properties();
        pro.setProperty("format", "json");
        pro.setProperty("strip_outer_array", "true");
        env
                .fromElements(
                        "{\"longitude\": \"116.405419\", \"city\": \" 北京\", \"latitude\": \"39.916927\"}"
                )
                .addSink(
                        DorisSink.sink(
                                DorisReadOptions.builder().build(),

                                DorisExecutionOptions.builder()
                                        .setBatchSize(3)
                                        .setBatchIntervalMs(0L)
                                        .setMaxRetries(3)
                                        .setStreamLoadProp(pro).build(),
                                DorisOptions.builder()
                                        .setFenodes("centos201:8030")
                                        .setTableIdentifier("example_db.table2")
                                        .setUsername("root")
                                        .setPassword("123456").build()
                        ));
// .addSink(
// DorisSink.sink(
// DorisOptions.builder()
// .setFenodes("FE_IP:8030")
// .setTableIdentifier("db.table")
// .setUsername("root")
// .setPassword("").build()
// ));
        env.execute(); */
    }
}
