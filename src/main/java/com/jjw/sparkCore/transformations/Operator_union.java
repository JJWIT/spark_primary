package com.jjw.sparkCore.transformations;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
/**
 * union  后的分区数是unionRDD分区的总和
 * @author root
 *
 */
public class Operator_union {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("union");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1,2,3),3);
		JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(4,5,6),2);
		JavaRDD<Integer> union = rdd1.union(rdd2);
		System.out.println("union.partitions().size()---"+union.partitions().size());
		union.foreach(new VoidFunction<Integer>() {
			
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
