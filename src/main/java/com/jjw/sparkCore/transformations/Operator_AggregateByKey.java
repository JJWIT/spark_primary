package com.jjw.sparkCore.transformations;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;


public class Operator_AggregateByKey {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("AggregateOperator")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		List<Tuple2<Integer,Integer>> dataList =  new ArrayList<Tuple2<Integer,Integer>>();
		dataList.add(new Tuple2<Integer, Integer>(1,99));
		dataList.add(new Tuple2<Integer, Integer>(2,78));
		dataList.add(new Tuple2<Integer, Integer>(1,89));
		dataList.add(new Tuple2<Integer, Integer>(2,3));
		dataList.add(new Tuple2<Integer, Integer>(3,3));
		dataList.add(new Tuple2<Integer, Integer>(3,30));
		
		JavaPairRDD<Integer, Integer> dataRdd = sc.parallelizePairs(dataList,2);
		dataRdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<Integer,Integer>>, Iterator<Tuple2<Integer,Integer>>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<Integer, Integer>> call(Integer index,
					Iterator<Tuple2<Integer, Integer>> iter) throws Exception {
				List<Tuple2<Integer, Integer>> list = new ArrayList<Tuple2<Integer, Integer>>();
				while(iter.hasNext()){
					/**
					 * 
					 */
					System.out.println("partitions --"+index+",value ---"+iter.next());
				}
				return list.iterator();
			}
		}, true).collect();
		System.out.println("*****************");
		JavaPairRDD<Integer, Integer> aggregateByKey = dataRdd.aggregateByKey(80,
				new Function2<Integer, Integer, Integer>() {
			/**
			 * 合并在同一个partition中的值，v1的数据类型为zeroValue的数据类型，v2的数据类型为原value的数据类型  
			 * 这里取出来一个分区中相同的key对应的value 
			 * 
			 * 分区0：
			 * (1,99)
			 * (1,89)
			 * (2,78) 
			 * 
			 * 
			 * 分区1：
			 * (2,3)
			 * (3,3) 
			 * (3,30)
			 * 
			 * combiner:
			 *  0：
			 *  (1,99)
			 *  (2,80)
			 *  1:
			 *  (2,80)
			 *  (3,80)
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer t1, Integer t2) throws Exception {
				System.out.println("seq: " + t1 + "\t " + t2);
				return Math.max(t1, t2);
			}
		},new Function2<Integer, Integer, Integer>() {

			/**
			 * 合并不同partition中的值，v1，v2的数据类型为zeroValue的数据类型  
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer t1, Integer t2) throws Exception {
				System.out.println("comb: " + t1 + "\t " + t2);
				return t1+t2;
			}
			
		});
		List<Tuple2<Integer,Integer>> resultRdd = aggregateByKey.collect();
		 for (Tuple2<Integer, Integer> tuple2 : resultRdd) {
			System.out.println(tuple2._1+"\t"+tuple2._2);
		}
	}
}
