package com.jjw.sparkCore.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * count
 * 返回结果集中的元素数，会将结果回收到Driver端。
 *
 */
public class Operator_count {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("collect");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> lines = jsc.textFile("./words.txt");
		long count = lines.count();
		System.out.println(count);
		jsc.stop();
	}
}
