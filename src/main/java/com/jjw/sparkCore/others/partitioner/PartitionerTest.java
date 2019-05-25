package com.jjw.sparkCore.others.partitioner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;
/**
 * 自定义分区器
 * @author root
 *
 */
public class PartitionerTest {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("partitioner");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaPairRDD<Integer, String> nameRDD = sc.parallelizePairs(Arrays.asList(
				new Tuple2<Integer, String>(1,"zhangsan"),
				new Tuple2<Integer, String>(2,"lisi"),
				new Tuple2<Integer, String>(3,"wangwu"),
				new Tuple2<Integer, String>(4,"zhaoliu"),
				new Tuple2<Integer, String>(5,"shunqi"),
				new Tuple2<Integer, String>(6,"zhouba")
			), 2);
		
		nameRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<Integer,String>>, Iterator<String>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(Integer index, Iterator<Tuple2<Integer, String>> iter) throws Exception {
				List<String> list = new ArrayList<String>();
				while(iter.hasNext()){
					System.out.println("nameRDD partitionID = "+index+" , value = "+iter.next());
				}
				return list.iterator();
			}
		}, true).collect();
		System.out.println("******************************");
		
		JavaPairRDD<Integer, String> partitionRDD = nameRDD.partitionBy(new Partitioner() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			/**
			 * 返回你想要创建分区的个数
			 */
			public int numPartitions() {
				return 2;
			}
			
			@Override
			/**
			 * 对输入的key做计算，然后返回该key的分区ID，范围一定是0到numPartitions-1
			 */
			public int getPartition(Object key) {
				int i = (int)key;
				if(i%2==0){
					return 0;
				}
				return 1;
			}
		});
		partitionRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<Integer,String>>, Iterator<String>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(Integer index, Iterator<Tuple2<Integer, String>> iter) throws Exception {
				List<String> list = new ArrayList<String>();
				while(iter.hasNext()){
					System.out.println("partitionRDD partitionID = "+index+" , value = "+iter.next());
				}
				return list.iterator();
			}
		}, true).collect();
		
		sc.stop();
	}
}
