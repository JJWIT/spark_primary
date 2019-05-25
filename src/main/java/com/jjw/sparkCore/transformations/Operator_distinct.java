package com.jjw.sparkCore.transformations;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class Operator_distinct {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("distinct");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<Integer> parallelize = sc.parallelize(Arrays.asList(1,2,3,3,4,4));
		JavaRDD<Integer> result = parallelize.mapToPair(new PairFunction<Integer, Integer, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, Integer> call(Integer t) throws Exception {
				return new Tuple2<Integer, Integer>(t, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		}).map(new Function<Tuple2<Integer,Integer>,Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Tuple2<Integer, Integer> v1) throws Exception {
				return v1._1;
			}

		});
		result.foreach(new VoidFunction<Integer>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer t) throws Exception {
				System.out.println(t);
			}
		});
		
		JavaRDD<Integer> distinct = parallelize.distinct();
		distinct.foreach(new VoidFunction<Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer t) throws Exception {
				System.out.println("distinct---"+t);
			}
		});
		sc.stop();
	}
}
