package com.jjw.sparkCore.transformations;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;
/**
 * 当调用类型（K，V）和（K，W）的数据集时，返回一个数据集（K，（Iterable <V>，Iterable <W>））元组。此操作也被称做groupWith。
 * @author root
 *
 */
public class Operator_cogroup {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("ReduceOperator")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Tuple2<String,String>> studentsList = Arrays.asList(
				new Tuple2<String,String>("1","zhangsan"),
				new Tuple2<String,String>("2","lisi"),
				new Tuple2<String,String>("2","wangwu"),
				new Tuple2<String,String>("3","maliu"));
		
		/**
		 * 1 zhangsan 100
		 * 1 zhangsan 100
		 * 2 lisi 90
		 * 2 lisi 60
		 * 3 wagnwu 80
		 * 3 wangwu 50
		 */
		List<Tuple2<String,String>> scoreList = Arrays.asList(
				new Tuple2<String,String>("1","100"),
				new Tuple2<String,String>("2","90"),
				new Tuple2<String,String>("3","80"),
				new Tuple2<String,String>("1","1000"),
				new Tuple2<String,String>("2","60"),
				new Tuple2<String,String>("3","50"));
		
		JavaPairRDD<String,String> students = sc.parallelizePairs(studentsList);
		JavaPairRDD<String,String> scores = sc.parallelizePairs(scoreList);
		
		// cogroup 与 join不同！
		// 相当于，一个key join上所有value，都给放到一个Iterable里面去！
		JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> studentScores = students.cogroup(scores);
		
		
		studentScores.foreach(new VoidFunction<Tuple2<String,Tuple2<Iterable<String>,Iterable<String>>>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(
					Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>> tuple)
					throws Exception {
				System.out.println(tuple);
//				System.out.println("student id : " + tuple._1);
//				System.out.println("student name : " + tuple._2._1);
//				System.out.println("student score : " + tuple._2._2);
			}
		});
		
//		System.out.println(studentScores.collect());
		
		sc.close();
	}
}
