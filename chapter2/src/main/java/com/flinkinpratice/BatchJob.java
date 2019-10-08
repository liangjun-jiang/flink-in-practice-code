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

package com.flinkinpratice;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Flink Word Count Batch Job.
 */
public class BatchJob {

	public static void main(String[] args) throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.fromElements(WORDS) // Specify the data source
			.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {  // use regular expression to split words
				@Override
				public void flatMap(String s, org.apache.flink.util.Collector<Tuple2<String, Integer>> collector) throws Exception {
					String[] splits = s.toLowerCase().split("\\W+");

					for (String split : splits) {
						if (split.length() > 0) {
							collector.collect(new Tuple2<>(split, 1));
						}
					}
				}
			})
			.groupBy(0) // group same word
			.reduce(new ReduceFunction<Tuple2<String, Integer>>() {  // count word
				@Override
				public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
					return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
				}
			})
			.print(); // print to stdout
	}

	private static final String[] WORDS = new String[]{
			"To be, or not to be,--that is the question:--",
			"Whether 'tis nobler in the mind to suffer",
			"The slings and arrows of outrageous fortune",
			"Or to take arms against a sea of troubles,",
			"And by opposing end them?--To die,--to sleep,--"
	};
}
