package com.jjw.sparkCore.actions;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
/**
 * take
 * 
 * @author root
 *
 */
public class Operator_takeAndFirst {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("take");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		/*
		JavaRDD<Tuple2<Integer,String>> parallelize = jsc.parallelize(
				Arrays.asList(
						new Tuple2<Integer,String>(1,"a"),
						new Tuple2<Integer,String>(2,"b"),
						new Tuple2<Integer,String>(3,"c"),
						new Tuple2<Integer,String>(4,"d")
						)
				);
		List<Tuple2<Integer,String>> take = parallelize.take(2);
		for(Tuple2<Integer,String> s:take){
			System.out.println(s);
		}
		jsc.stop();
		*/
		
		JavaRDD<String> parallelize = jsc.parallelize(Arrays.asList("a","b","c","d"));
		List<String> take = parallelize.take(2);
		String first = parallelize.first();
		for(String s:take){
			System.out.println(s);
		}
		jsc.stop();
	}
}
