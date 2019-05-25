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
 * coalesce减少分区
 * 第二个参数是减少分区的过程中是否产生shuffle，true是产生shuffle，false是不产生shuffle，默认是false.
 * 如果coalesce的分区数比原来的分区数还多，第二个参数设置false，即不产生shuffle,不会起作用。
 * 如果第二个参数设置成true则效果和repartition一样，即coalesce(numPartitions,true) = repartition(numPartitions)
 * 
 * @author root
 *
 */
public class Operator_coalesce {
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
		JavaRDD<String> rdd2 = rdd1.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>(){

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
//		JavaRDD<String> coalesceRDD = rdd2.coalesce(2, false);//不产生shuffle
//		JavaRDD<String> coalesceRDD = rdd2.coalesce(2, true);//产生shuffle
		
		JavaRDD<String> coalesceRDD = rdd2.coalesce(4,false);//设置分区数大于原RDD的分区数且不产生shuffle，不起作用
//		System.out.println("coalesceRDD partitions length = "+coalesceRDD.partitions().size());
		
//		JavaRDD<String> coalesceRDD = rdd2.coalesce(4,true);//设置分区数大于原RDD的分区数且产生shuffle，相当于repartition
//		JavaRDD<String> coalesceRDD = rdd2.repartition(4);
		JavaRDD<String> result = coalesceRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(Integer partitionId, Iterator<String> iter)
					throws Exception {
				List<String> list = new ArrayList<String>();
				while(iter.hasNext()){
					list.add("coalesceRDD的分区索引:【"+partitionId+"】,值为：	"+iter.next());
					
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
