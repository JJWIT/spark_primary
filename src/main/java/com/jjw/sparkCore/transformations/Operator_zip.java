package com.jjw.sparkCore.transformations;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class Operator_zip {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("zip");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> nameRDD = sc.parallelize(Arrays.asList("zhangsan","lisi","wangwu"));
		JavaRDD<Integer> scoreRDD = sc.parallelize(Arrays.asList(100,200,300));
//		JavaRDD<Integer> scoreRDD = sc.parallelize(Arrays.asList(100,200,300,400));
		JavaPairRDD<String, Integer> zip = nameRDD.zip(scoreRDD);
		zip.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> tuple) throws Exception {
				System.out.println("tuple --- " + tuple);
			}
		});
		
//		JavaPairRDD<String, String> parallelizePairs = sc.parallelizePairs(Arrays.asList(
//				new Tuple2<String, String >("a","aaa"),
//				new Tuple2<String, String >("b","bbb"),
//				new Tuple2<String, String >("c","ccc")
//				));
//		JavaPairRDD<String, String> parallelizePairs1 = sc.parallelizePairs(Arrays.asList(
//				new Tuple2<String, String >("1","111"),
//				new Tuple2<String, String >("2","222"),
//				new Tuple2<String, String >("3","333")
//				));
//		JavaPairRDD<Tuple2<String, String>, Tuple2<String, String>> result = parallelizePairs.zip(parallelizePairs1);

		sc.stop();
	}
}
