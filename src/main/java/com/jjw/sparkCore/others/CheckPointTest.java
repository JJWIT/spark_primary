package com.jjw.sparkCore.others;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class CheckPointTest {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("checkpoint");
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setCheckpointDir("./checkpoint");
		JavaRDD<Integer> parallelize = sc.parallelize(Arrays.asList(1,2,3));
		parallelize.checkpoint();
		parallelize.count();
		sc.stop();
	
	}
}
