package com.jjw.sparkCore.actions;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

public class Operator_foreach {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("foreach");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<Integer> parallelize = sc.parallelize(Arrays.asList(1,2,3));
		parallelize.foreach(new VoidFunction<Integer>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer t) throws Exception {
				System.out.println(t);
			}
		});
		
		sc.stop();
	}
}
