package com.jjw.sparkCore.transformations;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class Operator_sample {
	public static void main(String[] args) {
		/*SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("sample");
		
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> lines = jsc.textFile("./words.txt");
		JavaPairRDD<String, Integer> flatMapToPair = lines.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {

			*//**
			 * 
			 *//*
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, Integer>> call(String t)
					throws Exception {
				List<Tuple2<String,Integer>> tupleList = new ArrayList<Tuple2<String,Integer>>();
				tupleList.add(new Tuple2<String,Integer>(t,1));
				return tupleList;
			}
		});
		JavaPairRDD<String, Integer> sampleResult = flatMapToPair.sample(true,0.3,4);
		sampleResult.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			*//**
			 * 
			 *//*
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println(t);
			}
		});
		
		jsc.stop();*/
	}
}
