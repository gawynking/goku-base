/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pgman.goku.streaming;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;

public class JavaStatefulNetworkWordCount {

  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {

    // Create the context with a 1 second batch size
    SparkConf sparkConf = new SparkConf()
            .setAppName("JavaStatefulNetworkWordCount")
            .setMaster("local[*]");
    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
    ssc.checkpoint(".");

    // Initial state RDD input to mapWithState
    @SuppressWarnings("unchecked")
    List<Tuple2<String, Integer>> tuples =
        Arrays.asList(new Tuple2<>("hello", 1), new Tuple2<>("world", 1));

    JavaPairRDD<String, Integer> initialRDD = ssc.sparkContext().parallelizePairs(tuples);


    JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
            "192.168.72.131",
            9999,
            StorageLevels.MEMORY_AND_DISK_SER_2
    );

    JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());

    JavaPairDStream<String, Integer> wordsDstream = words.mapToPair(
            new PairFunction<String, String, Integer>() {
              @Override
              public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s,1);
              }
            }
    );

    // DStream made of get cumulative counts that get updated in every batch
    JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> stateDstream =
        wordsDstream.mapWithState(StateSpec.function(new Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> call(String word, Optional<Integer> one, State<Integer> state) throws Exception {
                int sum = one.orElse(0) + (state.exists() ? state.get() : 0);
                Tuple2<String, Integer> output = new Tuple2<>(word, sum);
                state.update(sum);
                return output;
            }
        }).initialState(initialRDD));

    stateDstream.print();
    ssc.start();
    ssc.awaitTermination();
  }

}
