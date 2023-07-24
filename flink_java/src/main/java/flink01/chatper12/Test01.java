package flink01.chatper12;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Test01 {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String createDDL = "CREATE TABLE level (" +
                " time1 BIGINT, " +
                " uid STRING, " +
                " level BIGINT " +
                ") WITH (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'data/input/level.csv', " +
                " 'format' =  'csv' " +
                ")";
        TableResult result = tableEnv.executeSql(createDDL);
        result.print();
        Table aggTable = tableEnv.sqlQuery(
                "select  \n" +
                "    level\n" +
                "    , count(1) as uv\n" +
                "    , max(time1) as time1\n" +
                "from (\n" +
                "    select \n" +
                "        uid\n" +
                "        , level\n" +
                "        , time1\n" +
                "        , row_number() over (partition by uid order by time1 desc) rn \n" +
                "    from level\n" +
                ") tmp\n" +
                "where rn =1 \n" +
                "group by \n" +
                "    level");
        aggTable.execute().print();

        env.execute();
    }
}
