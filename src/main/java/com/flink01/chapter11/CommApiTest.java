package com.flink01.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class CommApiTest {

    public static void main(String[] args) {
        //这里和DataStream 的写法一样
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //方法一：通过StreamTableEnvironment创建表环境
        //StreamTableEnvironment 继承 TableEnvironment
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);


        //方法二：通过TableEnvironment创建表
        //基于blink版本的planner进行流处理
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                //注册执行模式：批处理还是流处理
                .inStreamingMode()
                //注册使用计划器：默认使用blink planner
                .useBlinkPlanner()
                .build();

        TableEnvironment environment = TableEnvironment.create(settings);
        //创建表
        //自定义目录名和库名
        //如果没有指定的话默认的是default_catalog.default_database.MyTable
        //指定完成的结果：custom_catalog.custom_database.MyTable
        //environment.useCatalog("custom_catalog");
        //environment.useDatabase("custom_database");
        //连接器表
        //environment.executeSql("CREATE TEMPORARY TABLE MyTable with ('connector'= ....)")
        //虚拟表
        //Table table = environment.sqlQuery("select * from MyTable ....");
        //environment.createTemporaryView("newTable",table);

        String createDDL = "CREATE TABLE clickTable (" +
                " user_name STRING, " +
                " url STRING, " +
                " ts BIGINT " +
                ") WITH (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'data/input/clicks.csv', " +
                " 'format' =  'csv' " +
                ")";

        TableResult result = environment.executeSql(createDDL);
        result.print();

        System.out.println("原始表");
        environment.sqlQuery("select * from clickTable").execute().print();

        //表查询
        //调用TABLE API
        Table clickTable = environment.from("clickTable");
        Table resultTable = clickTable.where($("user_name").isEqual("Bob"))
                .select($("user_name"), $("url"));
        resultTable.execute().print();

        //如果想只用sql查询的话，需要将Table Api转成sql
        //方法需要将resultTable注册到环境中
        //实际上就是创建一个虚拟表
        environment.createTemporaryView("ResultTable", resultTable);

        environment.sqlQuery("select * from ResultTable")
                .execute()
                .print();


        //执行聚合函数
        Table aggTable = environment.sqlQuery("select user_name,count(1) as tt from clickTable group by user_name");
        aggTable.execute().print();


        //输出表
        // 4. 创建一张用于输出的表
        String createOutDDL = "CREATE TABLE outTable (" +
                " url STRING, " +
                " user_name STRING " +
                ") WITH (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'output', " +
                " 'format' =  'csv' " +
                ")";

        environment.executeSql(createOutDDL);

        // 创建一张用于控制台打印输出的表
        String createPrintOutDDL = "CREATE TABLE printOutTable (" +
                " user_name STRING, " +
                " cnt BIGINT " +
                ") WITH (" +
                " 'connector' = 'print' " +
                ")";

        environment.executeSql(createPrintOutDDL);

        // 5. 输出表
//        resultTable.executeInsert("outTable");
//        resultTable.executeInsert("printOutTable");
        aggTable.executeInsert("printOutTable");

    }
}
