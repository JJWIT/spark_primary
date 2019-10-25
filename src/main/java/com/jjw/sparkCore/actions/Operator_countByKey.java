package com.jjw.sparkCore.actions;

import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
/**
 * countByKey
 * 
 * 作用到K,V格式的RDD上，根据Key计数相同Key的数据集元素。返回一个Map<K,Object>
 * @author root
 *
 */
public class Operator_countByKey {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("countByKey");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaPairRDD<Integer, String> parallelizePairs = sc.parallelizePairs(Arrays.asList(
				new Tuple2<Integer,String>(1,"a"),
				new Tuple2<Integer,String>(2,"b"),
				new Tuple2<Integer,String>(3,"c"),
				new Tuple2<Integer,String>(4,"d"),
				new Tuple2<Integer,String>(4,"e")
		));
		
		Map<Integer, Long> countByKey = parallelizePairs.countByKey();
		for(Entry<Integer, Long> entry : countByKey.entrySet()){
			System.out.println("key:"+entry.getKey()+"value:"+entry.getValue());
		}
		
		
	}
}
