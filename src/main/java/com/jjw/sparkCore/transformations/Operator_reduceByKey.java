package com.jjw.sparkCore.transformations;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class Operator_reduceByKey {
	public static void main(String[] args) {
		/*SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("reduceByKey");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> lines = jsc.textFile("./words.txt");
		JavaRDD<String> flatMap = lines.flatMap(new FlatMapFunction<String, String>() {

			*//**
			 * 
			 *//*
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String t) throws Exception {
				return Arrays.asList(t.split(" "));
			}
		});
		JavaPairRDD<String, Integer> mapToPair = flatMap.mapToPair(new PairFunction<String, String, Integer>() {

			*//**
			 * 
			 *//*
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String,Integer>(t,1);
			}
			
		});
		
		JavaPairRDD<String, Integer> reduceByKey = mapToPair.reduceByKey(new Function2<Integer,Integer,Integer>(){

			*//**
			 * 
			 *//*
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
			
		},10);
		reduceByKey.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			*//**
			 * 
			 *//*
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println(t);
			}
		});
		
		jsc.stop();*/
	}
}
