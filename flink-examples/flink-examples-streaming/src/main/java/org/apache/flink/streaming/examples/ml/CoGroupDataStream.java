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
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.NotOptimizedEndOfStreamWindows;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/** This is Java doc. */
public class CoGroupDataStream {
    private static final Logger LOG = LoggerFactory.getLogger(CoGroupDataStream.class);
    private static boolean optimized = false;
    private static long dataNum = 1000;

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

        if (params.has("o")) {
            optimized = true;
        }
        LOG.info("Optimized: " + optimized);

        DataStream<Tuple3<Integer, Integer, Double>> data1 = env.fromCollection(
                new DataGenerator(dataNum), Types.TUPLE(Types.INT, Types.INT, Types.DOUBLE));
        DataStream<Tuple3<Integer, Integer, Double>> data2 = env.fromCollection(
                new DataGenerator(dataNum), Types.TUPLE(Types.INT, Types.INT, Types.DOUBLE));

        DataStream<Integer> resultStream;
        if (optimized) {
            resultStream = data1.coGroup(data2)
                    .where(tuple -> tuple.f0)
                    .equalTo(tuple -> tuple.f0)
                    .window(GlobalWindows.createWithEndOfStreamTrigger())
                    .apply(new CustomCoGroupFunction());
        } else {
            resultStream = data1.coGroup(data2)
                    .where(tuple -> tuple.f0)
                    .equalTo(tuple -> tuple.f0)
                    .window(NotOptimizedEndOfStreamWindows.get())
                    .apply(new CustomCoGroupFunction());
        }
        resultStream.addSink(new CountingAndDiscardingSink<Integer>());

//        StreamGraph streamGraph = env.getStreamGraph();
//        System.out.println("\nStreamGraph********************************\n");
//        System.out.println(streamGraph.getStreamingPlanAsJSON());
//        JobGraph jobGraph = streamGraph.getJobGraph();
//        System.out.println("\nJobGraph***********************************\n");
//        System.out.println(JsonPlanGenerator.generatePlan(jobGraph));
//        JobExecutionResult executionResult = env.execute(streamGraph);

        JobExecutionResult executionResult = env.execute(taskName);
        double totalTimeMs = (double) executionResult.getNetRuntime(TimeUnit.MILLISECONDS);
        long count = executionResult.getAccumulatorResult(CountingAndDiscardingSink.COUNTER_NAME);
        System.out.println("==> Count: " + count + " time: " + totalTimeMs);
    }

    static class DataGenerator
            implements Iterator<Tuple3<Integer, Integer, Double>>, Serializable {
        private final long numValues;
        private long cnt = 0;

        DataGenerator(long numValues) {
            this.numValues = numValues;
        }

        @Override
        public boolean hasNext() {
            return cnt < numValues;
        }

        @Override
        public Tuple3<Integer, Integer, Double> next() {
            cnt++;
            return Tuple3.of((int) (2 * cnt), (int) (2 * cnt) + 1, 1.0);
        }
    }

    private static class CustomCoGroupFunction
            extends RichCoGroupFunction<
            Tuple3<Integer, Integer, Double>, Tuple3<Integer, Integer, Double>, Integer> {

        CustomCoGroupFunction() {}

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public void close() throws Exception {
            super.close();
        }

        @Override
        public void coGroup(
                Iterable<Tuple3<Integer, Integer, Double>> iterableA,
                Iterable<Tuple3<Integer, Integer, Double>> iterableB,
                Collector<Integer> collector) {
            collector.collect(1);
        }
    }

    private static class CountingAndDiscardingSink<T> extends RichSinkFunction<T> {
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
}
