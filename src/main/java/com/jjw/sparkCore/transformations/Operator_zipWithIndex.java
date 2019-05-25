package com.jjw.sparkCore.transformations;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;
/**
 * zipWithIndex 会将RDD中的元素和这个元素在RDD中的索引号（从0开始） 组合成（K,V）对
 * @author root
 *
 */
public class Operator_zipWithIndex {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("zipWithIndex");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> nameRDD = sc.parallelize(Arrays.asList("zhangsan","lisi","wangwu"));
		JavaPairRDD<String, Long> zipWithIndex = nameRDD.zipWithIndex();
		zipWithIndex.foreach(new VoidFunction<Tuple2<String,Long>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Long> t) throws Exception {
				System.out.println("t ---- "+ t);
			}
		});
//		JavaPairRDD<String, String> parallelizePairs = sc.parallelizePairs(Arrays.asList(
//				new Tuple2<String, String >("a","aaa"),
//				new Tuple2<String, String >("b","bbb"),
//				new Tuple2<String, String >("c","ccc")
//				));
//		JavaPairRDD<Tuple2<String, String>, Long> zipWithIndex2 = parallelizePairs.zipWithIndex();
//		zipWithIndex2.foreach(new VoidFunction<Tuple2<Tuple2<String,String>,Long>>() {
//
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void call(Tuple2<Tuple2<String, String>, Long> t)
//					throws Exception {
//				System.out.println(" t ----" + t);
//			}
//		});
		sc.stop();
	}
	
}
