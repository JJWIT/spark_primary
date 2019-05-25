package com.jjw.sparkCore.transformations;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

import com.google.common.base.Optional;

public class Operator_join {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("join");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaPairRDD<Integer, String> nameRDD = sc.parallelizePairs(Arrays.asList(
					new Tuple2<Integer, String>(0, "aa"),
					new Tuple2<Integer, String>(1, "a"),
					new Tuple2<Integer, String>(2, "b"),
					new Tuple2<Integer, String>(3, "c")
				));
		JavaPairRDD<Integer, Integer> scoreRDD = sc.parallelizePairs(Arrays.asList(
				new Tuple2<Integer, Integer>(1, 100),
				new Tuple2<Integer, Integer>(2, 200),
				new Tuple2<Integer, Integer>(3, 300),
				new Tuple2<Integer, Integer>(4, 400)
		));
		JavaPairRDD<Integer, Tuple2<String, Integer>> join = nameRDD.join(scoreRDD);
//		System.out.println("join.partitions().size()--------"+join.partitions().size());
//		join.foreach(new VoidFunction<Tuple2<Integer,Tuple2<String,Integer>>>() {
//			
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void call(Tuple2<Integer, Tuple2<String, Integer>> t)
//					throws Exception {
//				System.out.println(t);
//			}
//		});
//			
//		JavaPairRDD<Integer, Tuple2<String, Optional<Integer>>> leftOuterJoin = nameRDD.leftOuterJoin(scoreRDD);
//		System.out.println("leftOuterJoin.partitions().size()--------"+leftOuterJoin.partitions().size());
//		leftOuterJoin.foreach(new VoidFunction<Tuple2<Integer,Tuple2<String,Optional<Integer>>>>() {
//
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void call(
//					Tuple2<Integer, Tuple2<String, Optional<Integer>>> t)
//					throws Exception {
//				System.out.println(t);
//			}
//		});
		
//		JavaPairRDD<Integer, Tuple2<Optional<String>, Integer>> rightOuterJoin = nameRDD.rightOuterJoin(scoreRDD);
//		System.out.println("rightOuterJoin.partitions().size()--------"+rightOuterJoin.partitions().size());
//		rightOuterJoin.foreach(new VoidFunction<Tuple2<Integer,Tuple2<Optional<String>,Integer>>>() {
//			
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void call(Tuple2<Integer, Tuple2<Optional<String>, Integer>> t)
//					throws Exception {
//				System.out.println(t);
//			}
//		});
		
//		JavaPairRDD<Integer, Tuple2<Optional<String>, Optional<Integer>>> fullOuterJoin = nameRDD.fullOuterJoin(scoreRDD);
//		System.out.println("fullOuterJoin.partitions().size()--------"+fullOuterJoin.partitions().size());
//		
//		fullOuterJoin.foreach(new VoidFunction<Tuple2<Integer,Tuple2<Optional<String>,Optional<Integer>>>>() {
//
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void call(
//					Tuple2<Integer, Tuple2<Optional<String>, Optional<Integer>>> t)
//					throws Exception {
//				System.out.println(t);
//			}
//		});
		
		
	}
}
