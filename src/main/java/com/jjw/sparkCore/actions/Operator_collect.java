package com.jjw.sparkCore.actions;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * collect 
 * 将计算的结果作为集合拉回到driver端，一般在使用过滤算子或者一些能返回少量数据集的算子后，将结果回收到Driver端打印显示。
 *
 */
public class Operator_collect {
	public static void main(String[] args) {
		/**
		 * SparkConf对象中主要设置Spark运行的环境参数。
		 * 1.运行模式
		 * 2.设置Application name
		 * 3.运行的资源需求
		 */
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("collect");
		/**
		 * JavaSparkContext对象是spark运行的上下文，是通往集群的唯一通道。
		 */
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> lines = jsc.textFile("./words.txt");
		JavaRDD<String> resultRDD = lines.filter(new Function<String, Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String line) throws Exception {
				return !line.contains("shsxt");
			}
			
		});
		List<String> collect = resultRDD.collect();
		for(String s :collect){
			System.out.println(s);
		}
		
		jsc.stop();
	}
}
