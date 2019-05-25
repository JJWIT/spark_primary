package com.jjw.sparkCore.actions;

import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
/**
 * countByValue
 * 根据数据集每个元素相同的内容来计数。返回相同内容的元素对应的条数。
 * 
 * @author root
 *
 */
public class Operator_countByValue {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("countByKey");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaPairRDD<Integer, String> parallelizePairs = sc.parallelizePairs(Arrays.asList(
				new Tuple2<Integer,String>(1,"a"),
				new Tuple2<Integer,String>(2,"b"),
				new Tuple2<Integer,String>(2,"c"),
				new Tuple2<Integer,String>(3,"c"),
				new Tuple2<Integer,String>(4,"d"),
				new Tuple2<Integer,String>(4,"d")
		));
		
		Map<Tuple2<Integer, String>, Long> countByValue = parallelizePairs.countByValue();
		
		for(Entry<Tuple2<Integer, String>, Long> entry : countByValue.entrySet()){
			System.out.println("key:"+entry.getKey()+",value:"+entry.getValue());
		}
	}
}
