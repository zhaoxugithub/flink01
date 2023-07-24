package flink01.chapter09;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * flink 状态编程
 * 在map算子中计算数据的个数
 */
public class Flink01_State_Operator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.socketTextStream("localhost", 9999).map(new MyCountMapper())
                .print();
        env.execute();
    }

    /*
    为了自定义Flink的算子，我们可以重写Rich Function接口类，比如RichFlatMapFunction。使用Keyed State时，
    我们也可以通过重写Rich Function接口类，在里面创建和访问状态。对于Operator State，我们还需进一步实现CheckpointedFunction接口。
     */
    public static class MyCountMapper implements MapFunction<String, Long>, CheckpointedFunction {

        //申明一个变量用来存放总数
        private Long count = 0L;
        //列表状态
        private ListState<Long> state;

        //这个方法来自Map接口，是计算逻辑方法
        @Override
        public Long map(String value) throws Exception {
            count++;
            return count;
        }

        // Checkpoint时会调用这个方法，我们要实现具体的snapshot逻辑，比如将哪些本地状态持久化
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println("snapshotState....");
            state.clear();
            state.add(count);
        }

        // 初始化时会调用这个方法，向本地状态中填充数据. 每个子任务调用一次,也会在恢复状态时使用
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            System.out.println("initializeState...");
            state = context.getOperatorStateStore().getListState(new ListStateDescriptor<Long>("state", Long.class));
            System.out.println("over......" + state.get());
            for (Long aLong : state.get()) {
                System.out.println("----");
                count += aLong;
            }
        }
    }
}
