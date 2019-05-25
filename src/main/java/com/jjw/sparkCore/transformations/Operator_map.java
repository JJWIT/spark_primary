package com.jjw.sparkCore.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * map 
 * 通过传入的函数处理每个元素，返回新的数据集。
 * 特点：输入一条，输出一条。
 * 
 * 
 * @author root
 *
 */
public class Operator_map {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("map");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> line = jsc.textFile("./words.txt");
		/*JavaRDD<String> mapResult = line.map(new Function<String, String>() {

			*//**
			 * 
			 *//*
			private static final long serialVersionUID = 1L;

			@Override
			public String call(String s) throws Exception {
				return s+"~";
			} 
		});*/

		/*mapResult.foreach(new VoidFunction<String>() {

			*//**
			 *
			 *//*
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});*/

		line.map(x -> "start - " + x).foreach(x -> System.out.println(x));
		jsc.stop();
	}
}
