package com.jjw.sparkCore.others;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
/**
 * 广播变量：
 * 1.不能将一个RDD使用广播变量广播出去，因为RDD是不存数据的，可以将RDD的结果广播出去。
 * 2.广播变量只能在Driver端定义，不能在Executor端定义。
 * 3.在Driver端可以修改广播变量的值，在Executor端不能修改广播变量的值。
 * 
 * @author root
 *
 */

public class BroadCast {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("broadcast");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<String> list = Arrays.asList("hello bjsxt");
		//广播变量将list广播出去
		final Broadcast<List<String>> broadCastList = sc.broadcast(list);
		
		JavaRDD<String> lines = sc.textFile("./words.txt");
		JavaRDD<String> result = lines.filter(new Function<String, Boolean>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String s) throws Exception {
				return broadCastList.value().contains(s);
			}
		});
		result.foreach(new VoidFunction<String>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});
		
		sc.close();
		
	}
}
