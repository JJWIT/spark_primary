package com.jjw.sparkCore.transformations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import javax.xml.crypto.Data;

public class Operator_mapPartitionWithIndex {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("mapPartitionWithIndex");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<String> names = Arrays.asList("zhangsan1", "zhangsan2", "zhangsan3","zhangsan4");
		
		/**
		 * 这里的第二个参数是设置并行度,也是RDD的分区数，并行度理论上来说设置大小为core的2~3倍
		 */
		JavaRDD<String> parallelize = sc.parallelize(names, 3);
		/*JavaRDD<String> mapPartitionsWithIndex = parallelize.mapPartitionsWithIndex(
				new Function2<Integer, Iterator<String>, Iterator<String>>() {

			*//**
			 * 
			 *//*
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(Integer index, Iterator<String> iter)
					throws Exception {
				List<String> list = new ArrayList<String>();
				while(iter.hasNext()){
					String s = iter.next();
					list.add(s+"~");
					System.out.println("partition id is "+index +",value is "+s );
				}
				return list.iterator();
			}
		}, true);*/

		/*JavaRDD<String> mapPartitionsWithIndex = parallelize.mapPartitionsWithIndex((index, dataa, datab) -> {
			return null;
		}, true);

		mapPartitionsWithIndex.collect();*/
		sc.stop();
	}
}
