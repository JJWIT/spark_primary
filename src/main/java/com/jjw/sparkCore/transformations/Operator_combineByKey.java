package com.jjw.sparkCore.transformations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class Operator_combineByKey {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("combineByKey");
		JavaSparkContext sc = new JavaSparkContext(conf);
		 /**
		  * zhangsan 7
		  * lisi 9
		  * wangwu 5
		  */
		JavaPairRDD<String, Integer> parallelizePairs = sc.parallelizePairs(Arrays.asList(
					new Tuple2<String, Integer>("zhangsan", 1),
					new Tuple2<String, Integer>("zhangsan", 2),
					new Tuple2<String, Integer>("lisi",3),
					new Tuple2<String, Integer>("zhangsan", 4),
					new Tuple2<String, Integer>("wangwu", 5),
					new Tuple2<String, Integer>("lisi", 6)
				),2);
		parallelizePairs.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<String,Integer>>, Iterator<Tuple2<String,Integer>>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<String, Integer>> call(Integer index,
					Iterator<Tuple2<String, Integer>> iter) throws Exception {
				List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
				while(iter.hasNext()){
					Tuple2<String, Integer> next = iter.next();
					System.out.println("partitionindex ="+index+",value="+next);
					list.add(next);
				}
				return list.iterator();
			}

		}, true).collect();
		
		System.out.println("****************");
		JavaPairRDD<String, Integer> combineByKey = parallelizePairs.combineByKey(
				new Function<Integer, Integer>() {

			/**
			 * 把当前分区的第一个值当做v1,这里给每个partition相当于初始化值
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1) throws Exception {
				return v1;
			}
		}, new Function2<Integer, Integer, Integer>() {

			/**
			 * 合并在同一个partition中的值
			 * call 方法的第一个参数就是分区中的初始值，第二个参数是分区中的第二个值，将结果再赋值给第一个参数，以此类推。。。。。。
			 * 
			 * partition 0 : 
			 * 	(zhangsan,18) 
 			 * 	(zhangsan,19)
 			 * 	(lisi,20)
 			 *  0分区结果：(zhangsan,18~!19)
 			 *  		 (lisi,20~)
 			 * partition 1 :
 			 * 	(zhangsan,21)
 			 * 	(wangwu,22)
 			 * 	(lisi,23)
 			 *  1分区结果：(zhangsan,21~)
 			 *  		 (wangwu,22~)
 			 *  		 (lisi,23~)
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer s, Integer v) throws Exception {
				return s+v;
			}
		}, new Function2<Integer,Integer,Integer>() {

			/**
			 * 合并不同partition中的值
			 * partition 0 ：
			 * 	0分区结果：(zhangsan,18~!19)
 			 *  		 (lisi,20~)
 			 * partition 1 :
 			 *  1分区结果：(zhangsan,21~)
 			 *  		 (wangwu,22~)
 			 *  		 (lisi,23~)
 			 *  
 			 *  分区合并结果
 			 *  	(zhangsan,18~!19#21~)
 			 *  	(lisi,20~#23~)
 			 *   	(wangwu,22~)
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer s1, Integer s2) throws Exception {
				return s1+s2;
			}
		});
		combineByKey.foreach(new  VoidFunction<Tuple2<String,Integer>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> tuple) throws Exception {
				System.out.println(tuple);
			}
		});
		sc.stop();
	}
}
