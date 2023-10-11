package flink01.chapter09;

import flink01.chapter05.ClickSource;
import flink01.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class BufferingSinkExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                                                      .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                                                                                      .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp));
        stream.print("input");
        stream.addSink(new BufferingSink(10));
    }

    public static class BufferingSink implements SinkFunction<Event>, CheckpointedFunction {
        private int threshold;
        // 定义一个列表状态
        private transient ListState<Event> checkpointedState;
        // 定义一个本地变量
        private List<Event> bufferedElements;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<>();
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            checkpointedState.clear();
            // 把当前局部变量中的所有元素写入到检查点中
            for (Event bufferedElement : bufferedElements) {
                checkpointedState.add(bufferedElement);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            checkpointedState = context.getOperatorStateStore()
                                       .getListState(new ListStateDescriptor<Event>("buffered-elements", Types.POJO(Event.class)));
            if (context.isRestored()) {
                for (Event event : checkpointedState.get()) {
                    System.out.println(event.toString());
                }
            }
        }

        /**
         * 把
         *
         * @param value   The input record.
         * @param context Additional context about the input record.
         * @throws Exception
         */
        @Override
        public void invoke(Event value, Context context) throws Exception {
            bufferedElements.add(value);
            // 如果大小相同
            if (bufferedElements.size() == threshold) {
                for (Event bufferedElement : bufferedElements) {
                    // 数出到外部系统，这里用控制台模拟
                    System.out.println(bufferedElement);
                }
                System.out.println("=========输出完毕===========");
                // 清空本地列表
                bufferedElements.clear();
            }
        }
    }
}
