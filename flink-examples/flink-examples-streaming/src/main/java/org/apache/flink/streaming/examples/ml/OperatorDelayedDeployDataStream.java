package org.apache.flink.streaming.examples.ml;

import org.apache.flink.api.common.BatchShuffleMode;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.NotOptimizedEndOfStreamWindows;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class OperatorDelayedDeployDataStream {
    private static final Logger LOG = LoggerFactory.getLogger(OperatorDelayedDeployDataStream.class);
    private static boolean optimized = false;
    private static boolean batch = false;
    private static int dataNum = 1000;

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final String taskName = params.toString();
        Configuration config = new Configuration();
        config.setBoolean(DeploymentOptions.ATTACHED, true);

        if(params.has("h")){
            config.set(
                    ExecutionOptions.BATCH_SHUFFLE_MODE,
                    BatchShuffleMode.ALL_EXCHANGES_HYBRID_FULL);
        }

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(config);

        env.getConfig().enableObjectReuse();
        env.getConfig().disableGenericTypes();
        env.setParallelism(1);

        if(params.has("r")){
            env.setStateBackend(new EmbeddedRocksDBStateBackend());
        }

        if (params.has("b")) {
            batch = true;
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
            LOG.info("Set RuntimeExecutionMode BATCH.");
        } else {
            env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
            LOG.info("Set RuntimeExecutionMode STREAMING.");
        }

        if (params.has("cp")) {
            int checkpointInterval;
            checkpointInterval = Integer.parseInt(params.get("cp"));
            env.enableCheckpointing(checkpointInterval);
            env.getCheckpointConfig().setCheckpointStorage("file:/Users/rann/Documents/TempFile/checkpoint");
            LOG.info("Enable checkpoint.");
        }

        if (params.has("n")) {
            dataNum = Integer.parseInt(params.get("n"));
        }
        LOG.info("Number of records: " + dataNum);

        int keyNum = dataNum / 100;
        if (params.has("k")) {
            keyNum = Integer.parseInt(params.get("k"));
        }
        LOG.info("Number of keys: " + keyNum);

        if (params.has("o")) {
            optimized = true;
        }
        LOG.info("Optimized: " + optimized);

        DataStream<Tuple2<Integer, Double>> sourceStream = env.fromCollection(
                new BoundedDataGenerator(dataNum, keyNum), Types.TUPLE(Types.INT, Types.DOUBLE))
                .slotSharingGroup("Slot1");

        DataStream<Tuple2<Integer, Double>> unboundedSourceSteam = env.fromCollection(
                new DataGenerator(dataNum / 100), Types.TUPLE(Types.INT, Types.DOUBLE))
                .slotSharingGroup("Slot2");

        if(optimized){
            sourceStream
                    .keyBy(value -> value.f0)
                    .window(GlobalWindows.createWithEndOfStreamTrigger())
                    .aggregate(new MyAggregator()).name("Process1").slotSharingGroup("Slot3")
                    .connect(unboundedSourceSteam.keyBy(value -> value.f0))
                    .transform("Process2", Types.INT, new MyProcessOperator(true))
                    .slotSharingGroup("Slot4")
                    .addSink(new DiscardingSink<>()).slotSharingGroup("Slot4");
        }
        else{
            sourceStream
                    .keyBy(value -> value.f0)
                    .window(NotOptimizedEndOfStreamWindows.get())
                    .aggregate(new MyAggregator()).name("Process1").slotSharingGroup("Slot3")
                    .connect(unboundedSourceSteam.keyBy(value -> value.f0))
                    .transform("Process2", Types.INT, new MyProcessOperator(true))
                    .slotSharingGroup("Slot4")
                    .addSink(new DiscardingSink<>()).slotSharingGroup("Slot4");
        }

        JobExecutionResult executionResult = env.execute(taskName);
        double totalTimeMs = (double) executionResult.getNetRuntime(TimeUnit.MILLISECONDS);
        LOG.info(String.format("Runtime: %f ms.\n", totalTimeMs));
    }

    private static class MyAggregator implements
            AggregateFunction<
                    Tuple2<Integer, Double>,
                    Tuple2<Integer, Double>,
                    Tuple2<Integer, Double>>{
        @Override
        public Tuple2<Integer, Double> createAccumulator() {
            return new Tuple2<Integer, Double>(0, 0.0);
        }

        @Override
        public Tuple2<Integer, Double> add(Tuple2<Integer, Double> myData, Tuple2<Integer, Double> accData) {
            accData.f1 = accData.f1 + myData.f1;
            return accData;
        }

        @Override
        public Tuple2<Integer, Double> getResult(Tuple2<Integer, Double> result) {
            return result;
        }

        @Override
        public Tuple2<Integer, Double> merge(Tuple2<Integer, Double> accData, Tuple2<Integer, Double> acc1) {
            accData.f1 = accData.f1 + acc1.f1;
            return accData;
        }
    }

    static class DataGenerator
            implements Iterator<Tuple2<Integer, Double>>, Serializable {
        private final long numValues;
        private int cnt = 0;

        DataGenerator(int numValues) {
            this.numValues = numValues;
        }

        @Override
        public boolean hasNext() {
            return cnt < numValues;
        }

        @Override
        public Tuple2<Integer, Double> next() {
            return Tuple2.of(++cnt, 1.0);
        }
    }

    static class BoundedDataGenerator
            implements Iterator<Tuple2<Integer, Double>>, Serializable {
        private final long numValues;
        private int cnt = 0;
        private int numKeys;

        BoundedDataGenerator(int numValues, int numKeys) {
            this.numKeys = numKeys;
            this.numValues = numValues;
        }

        @Override
        public boolean hasNext() {
            return cnt < numValues;
        }

        @Override
        public Tuple2<Integer, Double> next() {
            return Tuple2.of(++cnt % numKeys, 1.0);
        }
    }

    public static class MyProcessOperator extends AbstractStreamOperator<Integer>
            implements TwoInputStreamOperator<Tuple2<Integer, Double>, Tuple2<Integer, Double>, Integer>,
            BoundedMultiInput {
        private transient ListState<Tuple2<Integer, Double>> checkpointedState;
//        private final List<Tuple2<Integer, Double>> bufferedElements;

        boolean stream1EndInput = false;
        boolean operatorOptimized;

        public MyProcessOperator(boolean operatorOptimized) {
            this.operatorOptimized = operatorOptimized;
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            super.snapshotState(context);
//            checkpointedState.clear();
//            for (Tuple2<Integer, Double> element : bufferedElements) {
//                checkpointedState.add(element);
//            }
        }

        @Override
        public void endInput(int inputId) throws Exception {
            stream1EndInput = true;
        }

        @Override
        public void processElement1(StreamRecord<Tuple2<Integer, Double>> element) throws Exception {
        }

        @Override
        public void processElement2(StreamRecord<Tuple2<Integer, Double>> element)
                throws Exception {
            Tuple2<Integer, Double> value = element.getValue();
            if (!operatorOptimized) {
                checkpointedState.add(value);
                if(stream1EndInput){
                    Iterator<Tuple2<Integer, Double>> iterator = checkpointedState
                            .get()
                            .iterator();
                    while (iterator.hasNext()) {
                        processUnboundedStreamRecord(iterator.next());
                    }
                    checkpointedState.clear();
                }
            }
            else{
                processUnboundedStreamRecord(value);
            }
//            if (!optimized && !batch) {
//                bufferedElements.add(value);
//                if(stream1EndInput){
//                    for (Tuple2<Integer, Double> bufferedElement : bufferedElements) {
//                        processUnboundedStreamRecord(bufferedElement);
//                    }
//                    bufferedElements.clear();
//                }
//            }
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            ListStateDescriptor<Tuple2<Integer, Double>> descriptor =
                    new ListStateDescriptor<>(
                            "buffered-elements",
                            TypeInformation.of(new TypeHint<Tuple2<Integer, Double>>() {}));

            checkpointedState = context.getOperatorStateStore().getListState(descriptor);

//            if (context.isRestored()) {
//                for (Tuple2<Integer, Double> element : checkpointedState.get()) {
//                    bufferedElements.add(element);
//                }
//            }
        }

        @Override
        public void close() throws Exception {
            super.close();
        }

        private void processUnboundedStreamRecord(Tuple2<Integer, Double> record) throws Exception {
//            Thread.sleep(1);
            output.collect(new StreamRecord<>(1));
        }
    }
}
