package com.jjw.sparkCore.actions;

import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

public class Operator_foreachPartition {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("foreachPartition");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("./words.txt",3);
		lines.foreachPartition(new VoidFunction<Iterator<String>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = -2302401945670821407L;

			@Override
			public void call(Iterator<String> t) throws Exception {
				System.out.println("创建数据库连接。。。");
				while(t.hasNext()){
					System.out.println(t.next());
				}
				
			}
		});
		
		sc.stop();
	}
}
