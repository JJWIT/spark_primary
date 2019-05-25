package com.jjw.sparkCore.transformations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
/**
 * repartition 
 * 减少或者增多分区，会产生shuffle.(多个分区分到一个分区中不会产生shuffle)
 * @author root
 *
 */
public class Operator_repartition {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("coalesce");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<String> list = Arrays.asList(
				"love1","love2","love3",
				"love4","love5","love6",
				"love7","love8","love9",
				"love10","love11","love12"
				);
		
		JavaRDD<String> rdd1 = sc.parallelize(list,3);
		JavaRDD<String> rdd2 = rdd1.mapPartitionsWithIndex(
				new Function2<Integer, Iterator<String>, Iterator<String>>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(Integer partitionId, Iterator<String> iter)
					throws Exception {
				List<String> list = new ArrayList<String>();
				while(iter.hasNext()){
					list.add("RDD1的分区索引:【"+partitionId+"】,值为："+iter.next());
				}
				return list.iterator();
			}
			
		}, true);
		JavaRDD<String> repartitionRDD = rdd2.repartition(1);
//		JavaRDD<String> repartitionRDD = rdd2.repartition(2);
//		JavaRDD<String> repartitionRDD = rdd2.repartition(6);
		JavaRDD<String> result = repartitionRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(Integer partitionId, Iterator<String> iter)
					throws Exception {
				List<String> list = new ArrayList<String>();
				while(iter.hasNext()){
					list.add("repartitionRDD的分区索引:【"+partitionId+"】,值为：	"+iter.next());
					
				}
				return list.iterator();
			}
			
		}, true);
		for(String s: result.collect()){
			System.out.println(s);
		}
		sc.stop();
	}

}
