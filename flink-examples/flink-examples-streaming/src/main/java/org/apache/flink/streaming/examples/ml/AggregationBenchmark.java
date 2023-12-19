/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.ml;

import org.apache.flink.api.common.BatchShuffleMode;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.NotOptimizedEndOfStreamWindows;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/** Entry class for benchmark execution. */
public class AggregationBenchmark {
    private static final Logger LOG = LoggerFactory.getLogger(AggregationBenchmark.class);
    private static boolean optimized = false;
    private static long dataNum = 1000;
    private static long keyNum = 10;
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        Configuration config = new Configuration();
        config.setBoolean(DeploymentOptions.ATTACHED, true);
        if(params.has("h")){
            config.set(ExecutionOptions.BATCH_SHUFFLE_MODE,
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
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
            LOG.info("Set RuntimeExecutionMode BATCH.");
        } else {
            env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
            LOG.info("Set RuntimeExecutionMode STREAMING.");
        }

        if (params.has("cp")) {
            int checkpointInterval;
            checkpointInterval = Integer.parseInt("cp");
            env.enableCheckpointing(checkpointInterval);
            env.getCheckpointConfig().setCheckpointStorage("file:/Users/rann/Documents/TempFile/checkpoint");
            LOG.info("Enable checkpoint.");
        }

        if (params.has("n")) {
            dataNum = Long.parseLong(params.get("n"));
        }
        LOG.info("Number of records: " + dataNum);

        keyNum = dataNum / 100;
        if (params.has("k")) {
            keyNum = Long.parseLong(params.get("k"));
        }
        LOG.info("Number of keys: " + keyNum);

        if (params.has("o")) {
            optimized = true;
        }
        LOG.info("Optimized: " + optimized);


        DataStreamSource<Tuple2<Long, Double>> events  = env.fromCollection(
                        new DataGenerator(dataNum, keyNum), Types.TUPLE(Types.LONG, Types.DOUBLE));
        if (optimized){
            events
                    .keyBy(value -> value.f0)
                    .window(GlobalWindows.createWithEndOfStreamTrigger())
                    .aggregate(new Aggregator())
                    .addSink(new CountingAndDiscardingSink());
        }
        else{
            events
                    .keyBy(value -> value.f0)
                    .window(NotOptimizedEndOfStreamWindows.get())
                    .aggregate(new Aggregator())
                    .addSink(new CountingAndDiscardingSink());
        }

        JobExecutionResult executionResult = env.execute();
        double totalTimeMs = (double) executionResult.getNetRuntime(TimeUnit.MILLISECONDS);
        long count = executionResult.getAccumulatorResult(CountingAndDiscardingSink.COUNTER_NAME);
        System.out.println("Count: " + count + " time: " + totalTimeMs + "ms");
    }

    public static class Aggregator
            implements AggregateFunction<Tuple2<Long, Double>, Tuple2<Long, Double>, Double> {

        @Override
        public Tuple2<Long, Double> createAccumulator() {
            return new Tuple2<>(0L, 0.0);
        }

        @Override
        public Tuple2<Long, Double> add(Tuple2<Long, Double> myData, Tuple2<Long, Double> accData) {
            accData.f1 = accData.f1 + myData.f1;
            return accData;
        }

        @Override
        public Double getResult(Tuple2<Long, Double> result) {
            return result.f1;
        }

        @Override
        public Tuple2<Long, Double> merge(Tuple2<Long, Double> accData, Tuple2<Long, Double> acc1) {
            accData.f1 = accData.f1 + acc1.f1;
            return accData;
        }
    }

    /**
     * A stream sink that counts the number of all elements. The counting result is stored in an
     * {@link org.apache.flink.api.common.accumulators.Accumulator} specified by {@link
     * #COUNTER_NAME} and can be acquired by {@link
     * JobExecutionResult#getAccumulatorResult(String)}.
     *
     * @param <T> The type of elements received by the sink.
     */
    public static class CountingAndDiscardingSink<T> extends RichSinkFunction<T> {
        public static final String COUNTER_NAME = "numElements";

        private static final long serialVersionUID = 1L;

        private final LongCounter numElementsCounter = new LongCounter();

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            getRuntimeContext().addAccumulator(COUNTER_NAME, numElementsCounter);
        }

        @Override
        public void invoke(T value, Context context) {
            numElementsCounter.add(1L);
        }
    }

    static class DataGenerator
            implements Iterator<Tuple2<Long, Double>>, Serializable {
        private final long numValues;
        long cnt = 0;
        private final long numKeys;

        DataGenerator(long numValues, long numKeys) {
            this.numKeys = numKeys;
            this.numValues = numValues;
        }



        @Override
        public boolean hasNext() {
            return cnt < numValues;
        }

        @Override
        public Tuple2<Long, Double> next() {
            return Tuple2.of(++cnt % numKeys, 1.0);
        }
    }
}
