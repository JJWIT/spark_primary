package com.jjw.sparkCore.transformations;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
/**
 * subtract
 * 取一个RDD与另一个RDD的差集
 * 
 * @author root
 *
 */
public class Operator_subtract {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("subtract");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a","b","c"));
		JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("a","e","f"));
		//subtract取差集，两个RDD的类型要一致。
		JavaRDD<String> subtract = rdd1.subtract(rdd2);
//		JavaRDD<String> subtract = rdd2.subtract(rdd1);
		subtract.foreach(new VoidFunction<String>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});
		sc.stop();
	}
}
