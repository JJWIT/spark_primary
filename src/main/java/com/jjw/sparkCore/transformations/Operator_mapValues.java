package com.jjw.sparkCore.transformations;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;
/**
 * mapValues是针对[K,V]中的V值进行map操作。
 *
 */
public class Operator_mapValues {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("mapValues");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaPairRDD<String, Integer> parallelizePairs = sc.parallelizePairs(Arrays.asList(
					new Tuple2<String,Integer>("a",100),
					new Tuple2<String,Integer>("b",200),
					new Tuple2<String,Integer>("c",300)
				));
		JavaPairRDD<String, String> mapValues = parallelizePairs.mapValues(new Function<Integer, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Integer s) throws Exception {
				System.out.println("values ------ "+s);
				return s+"~";
			}
		});
		mapValues.foreach(new VoidFunction<Tuple2<String,String>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, String> arg0) throws Exception {
				System.out.println(arg0);
			}
		});
		sc.stop();
	}
}
