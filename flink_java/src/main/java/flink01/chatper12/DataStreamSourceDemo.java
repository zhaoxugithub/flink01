package flink01.chatper12;

public class DataStreamSourceDemo {

    public static void main(String[] args) throws Exception {
/*         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties properties = new Properties();
        properties.put("fenodes", "centos201:8030");
        properties.put("username", "root");
        properties.put("password", "123456");
        properties.put("table.identifier", "example_db.table1");
        DataStreamSource<List<?>> dataStreamSource = env.addSource(new DorisSourceFunction(
                        new DorisStreamOptions(properties),
                        new SimpleListDeserializationSchema()
                )
        );
        dataStreamSource.print();
        env.execute(); */
    }
}
