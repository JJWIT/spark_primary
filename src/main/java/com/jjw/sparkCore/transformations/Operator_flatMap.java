package com.jjw.sparkCore.transformations;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * flatMap
 * 输入一条数据，输出0到多条数据。
 * @author root
 *
 */
public class Operator_flatMap {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("flatMap");

		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> lines = jsc.textFile("./words.txt");
		JavaRDD<String> flatMapResult = lines.flatMap(new FlatMapFunction<String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String s) throws Exception {
				
				return Arrays.asList(s.split(" "));
			}
			
		});
		flatMapResult.foreach(new VoidFunction<String>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});
		
		jsc.stop();
	}
}
