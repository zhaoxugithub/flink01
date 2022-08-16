package flink01.chatper12;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 运用flinkSQL 进行对doris读写操作
 */
public class SQLDemo {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql("CREATE TABLE flink_doris (\n" +
                " siteid INT,\n" +
                " citycode SMALLINT,\n" +
                " username STRING,\n" +
                " pv BIGINT\n" +
                " ) \n" +
                " WITH (\n" +
                " 'connector' = 'doris',\n" +
                " 'fenodes' = 'centos201:8030',\n" +
                " 'table.identifier' = 'example_db.table1',\n" +
                " 'username' = 'root',\n" +
                " 'password' = '123456'\n" +
                ")\n");
        // 读取数据
        tableEnv.executeSql("select * from flink_doris").print();
        // 写入数据
        tableEnv.executeSql("insert into flink_doris(siteid,username,pv)values(23,'wuyanzu',3)")
                .print();
    }


    public static void test() {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.setParallelism(1);


    }

}
