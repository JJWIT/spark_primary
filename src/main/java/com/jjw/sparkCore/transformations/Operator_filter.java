package com.jjw.sparkCore.transformations;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
/**
 * filter
 * 过滤符合符合条件的记录数，true的保留，false的过滤掉。
 *
 */
public class Operator_filter {
	public static void main(String[] args) {
		/**
		 * SparkConf对象中主要设置Spark运行的环境参数。
		 * 1.运行模式
		 * 2.设置Application name
		 * 3.运行的资源需求
		 */
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("filter");
		/**
		 * JavaSparkContext对象是spark运行的上下文，是通往集群的唯一通道。
		 */
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> lines = jsc.textFile("./words.txt");
		/*JavaRDD<String> resultRDD = lines.filter(new Function<String, Boolean>() {

			*//**
			 * 
			 *//*
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String line) throws Exception {
				return !line.contains("shsxt");
			}
			
		});
		
		resultRDD.foreach(new VoidFunction<String>() {
			
			*//**
			 * 
			 *//*
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String line) throws Exception {
				System.out.println(line);
			}
		});*/

		lines.filter(line -> line.contains("Spark")).foreach(x -> System.out.println(x));
		jsc.stop();
	}
}
