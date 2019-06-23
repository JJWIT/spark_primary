package com.jjw.sparkCore.transformations;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

public class Operator_mappartitions {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("mappartition");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("./words.txt",3);
		
//		lines.map(new Function<String, String>() {
//
//			*
//			 * 
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public String call(String arg0) throws Exception {
//				System.out.println("创建数据库链接对象。。。。");
//				System.out.println("插入数据库。。。"+arg0);
//				System.out.println("关闭数据库链接。。。");
//				return arg0;
//			}
//		}).collect();
//		
		
		JavaRDD<String> mapPartitions = lines.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(Iterator<String> t) throws Exception {

				System.out.println("创建数据库连接对象。。。。");
				List<String> list  = new ArrayList<>();
				while(t.hasNext()) {
					list.add(t.next());
				}
				System.out.println("批量插入数据库");
				System.out.println("关闭数据库连接");
				return list.iterator();
			}
		});
//		mapPartitions.collect();

		JavaRDD<String> stringJavaRDD = lines.mapPartitions(ite -> {
			System.out.println("创建数据库连接对象。。。。");
			List<String> list  = new ArrayList<>();
			while(ite.hasNext()) {
				System.out.println(ite.next());
				list.add(ite.next());
			}
			System.out.println("批量插入数据库");
			System.out.println("关闭数据库连接");
			return list.iterator();
		});
		stringJavaRDD.collect();// 触发
		sc.stop();
		
	}
}
